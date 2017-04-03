package com.datametl.tasks;

import com.datametl.exception.JSONParsingException;
import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import com.datametl.logging.Logger;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.*;
import java.nio.channels.Channels;
import java.util.*;

/**
 * Created by TseAndy on 2/6/17.
 */
public class ExtractTask implements Task{

    private JobState returnCode = JobState.NOT_STARTED;
    private SubJob parent = null;
    private JSONObject etlPacket;

    private ArrayList<Object> fieldName = new ArrayList<Object>();
    private List<List<Object>> rows = new ArrayList<List<Object>>();

    private String filePath;
    private String fileType;
    private int docToRead;
    private long bytePos;
    private long lastBytePos;
    private Logger log;

    /**
     * Constructor
     * <p>
     * Sets the returnCode to NOT_STARTED
     */
    public ExtractTask(Logger log) {
        this.log = log;
    }


    /**
     * Parses through the file by the file type given in the ETLPacket.
     * <p>
     * Obtains the filePath,fileType,docsToRead, and bytePos and determines
     * the necessary method for extraction of file. After succession,
     * it will create a new RulesEngineTask and pass the ETLPacket.
     * Then it empties the content array in the ETLPacket.
     */
    public void apply() {

//      Obtain necessary data from ETLPacket for extraction
        this.returnCode = JobState.RUNNING;
        this.etlPacket = parent.getETLPacket();
        this.filePath = etlPacket.getJSONObject("source").getString("path");
        this.fileType = etlPacket.getJSONObject("source").getString("file_type");
        this.docToRead = etlPacket.getInt("documents_to_read");
        this.bytePos = etlPacket.getLong("current_byte_position");

//      Determines which extraction method based on file type
//      Sets JobState to failed if fileType was not specified
        if(this.fileType.equals("csv")){
            extractCSV();
        }else if(this.fileType.equals("json")){
            try {
                extractJSON();
            }catch(JSONParsingException e){
                e.printStackTrace();
            }
        }else if(this.fileType.equals("xml")){
            extractXML();
        }else{
            this.returnCode = JobState.FAILED;
        }

//      Creates a new RulesEngineTask if extraction was successful
        Task rules = new RulesEngineTask(log);
        SubJob newRulesSubJob = new SubJob(rules);

//      INFO: Give RulesEngine a copy and reset the contents
        newRulesSubJob.setETLPacket(new JSONObject(etlPacket.toString()));
        JSONArray empty = new JSONArray();
        etlPacket.getJSONObject("data").put("contents", empty);

        boolean status = parent.getParent().addSubJob(newRulesSubJob);

        returnCode = JobState.SUCCESS;

    }

    /**
     * Returns the returnCode of the JobState
     *
     * @return returnCode of ExtractTask
     */
    public JobState getResult() {
        return this.returnCode;
    }

    /**
     * Sets the parent of ExtractTask
     *
     * @param parent PLACEHOLDER
     */
    public void setParent(SubJob parent) {
        this.parent = parent;
    }

    /**
     * Returns the parent of ExtractTask
     *
     * @return parent of ExtractTask
     */
    public SubJob getParent() {
        return parent;
    }

    /**
     * Parses through the CSV file by chunks and stores the data into arrays.
     * <p>
     * The method uses a CSVReader and will separate the values with its default delimiters.
     * It will skip the necessary lines when parsing a new chunk and stores each line into an array.
     * Once the file parses the chunk specified, it will call inputETLPacket to put the content into it.
     * Should the parsing fail, the jobState is set for failed.
     */
    private void extractCSV() {

        int count=0;
        int linesToSkip=0;
        BufferedReader buff;
        RandomAccessFile raf = null;

//      Determines whether or not if there is linesRead in the ETLPacket
//      If so, then it will skip lines in the CSV based on the value.
//      Else, it will put linesRead into the ETLPacket.
        if(this.etlPacket.has("linesRead")){
            linesToSkip = (Integer)this.etlPacket.get("linesRead");
        }else{
            this.etlPacket.put("linesRead",0);
        }

        try{

//          A method to allow CSVParser to take in RandomAccessFile.
            raf = new RandomAccessFile(new File(this.filePath),"r");
            InputStream is = Channels.newInputStream(raf.getChannel());
            InputStreamReader isr = new InputStreamReader(is);
            buff = new BufferedReader(isr);

//          CSVReader will skip lines of the file and will use the default delimiters.
            CSVReader reader = new CSVReader(buff, CSVParser.DEFAULT_SEPARATOR,CSVParser.DEFAULT_QUOTE_CHARACTER,linesToSkip);
            Object[] nextLine;

//          Each nextLine is a row in CSV
            while((nextLine=reader.readNext())!=null){
                if(nextLine!=null) {
                    List<Object> listLine = Arrays.asList(nextLine);
                    if (this.fieldName.isEmpty()) {
                        //First line must be field names
                        this.fieldName = new ArrayList<Object>(listLine);
                    } else {

                        this.rows.add(listLine);
                    }
                }

                count++;
                if(count>this.docToRead){ // LinesToSkip is based on index 1
                    break;
                }

                this.lastBytePos = raf.getFilePointer();
            }


            reader.close();
            inputETLPacket();
            this.etlPacket.put("current_byte_position", (this.lastBytePos));
            this.etlPacket.put("linesRead",linesToSkip+count-1); //LinesToSkip is based on index 1
        }catch(IOException e){
            e.printStackTrace();
            this.returnCode = JobState.FAILED;
        }
    }

    /**
     * Parses through the JSON file by chunks and stores the data into arrays.
     * <p>
     * The method will append lines into StringBuffer and call convertToJSONObject.
     * It will continue to append until the string can be converted to JSONObject
     * or the loop increments to the designated breakpoint. Each succession will
     * increment a counter and stop the loop when the entire chunk defined is read.
     *
     * @throws JSONParsingException
     */
    private void extractJSON() throws JSONParsingException {

        int count = 0;
        int breakCount=0;
        int maxBreakCount=500;

        try{

            RandomAccessFile randomAccessFile = new RandomAccessFile(this.filePath,"r");

            StringBuffer stringBuffer = new StringBuffer();
            String line;

//          Define line based on whether if the pointer is at the beginning of the file or not.
            if(this.bytePos==0){
                line = randomAccessFile.readLine();
            }else{
                randomAccessFile.seek(bytePos);
                line = randomAccessFile.readLine();
            }

//          Will attempt to append lines into StringBuffer and form a JSONObject
//          It is unknown whether the JSONObject is too long or valid so a breakpoint is set in place.
            while(breakCount<maxBreakCount) {
                boolean convertSuccess=false;

//              Will close and exit the file if it has reached the end.
                if(randomAccessFile.getFilePointer()>= randomAccessFile.length()){
                    this.lastBytePos=randomAccessFile.getFilePointer();
                    this.etlPacket.put("current_byte_position",this.lastBytePos);
                    log.debug("END OF FILE - CLOSING");
                    randomAccessFile.close();
                    break;
                }


                //If first line has '[', ignore and add to stringBuffer
                if (line.charAt(0) == '[') {

                    //Appends line without [
                    stringBuffer.append(line.substring(1));
                    line = line.substring(1);
                    String tempLine;

                    if (stringBuffer.charAt(stringBuffer.length() - 1) == ',') {
                        tempLine = stringBuffer.substring(0, stringBuffer.length() - 1);
                        convertSuccess=convertToJSONObject(tempLine);

                    }
                    else if(stringBuffer.charAt(stringBuffer.length() - 1) == ']') {
                        tempLine = stringBuffer.substring(0, stringBuffer.length() - 1);
                        convertSuccess=convertToJSONObject(tempLine);
                    }
                    if(convertSuccess){
                        count++;
                        breakCount=0;
                        stringBuffer.setLength(0);
                    }

                } else {
                    //If not the first line, append
                    stringBuffer.append(randomAccessFile.readLine());
                    String tempLine;

                    if (stringBuffer.charAt(stringBuffer.length() - 1) == ',') {
                        tempLine = stringBuffer.substring(0, stringBuffer.length() - 1);
                        convertSuccess=convertToJSONObject(tempLine);
                    }
                    else if(stringBuffer.charAt(stringBuffer.length() - 1) == ']') {
                        tempLine = stringBuffer.substring(0, stringBuffer.length() - 1);
                        convertSuccess=convertToJSONObject(tempLine);
                    }

                    if(convertSuccess){
                        count++;
                        breakCount=0;
                        stringBuffer.setLength(0);
                    }

                }

                if(count>=this.docToRead){
                    break;
                }else{
                    this.lastBytePos = randomAccessFile.getFilePointer();
                }

                //Increment counter to determine if valid
                breakCount++;

            }

//          If the JSON is too long or invalid, it will close the file and set returnCode to fail.
            if(breakCount>=maxBreakCount){
                randomAccessFile.close();
                this.returnCode = JobState.FAILED;
                throw new JSONParsingException("Parsing JSONObject timed out. File is either invalid, formatted incorrectly or the object is too large.");
            }else{
                randomAccessFile.close();
                inputETLPacket();
            }

            this.etlPacket.put("current_byte_position", (this.lastBytePos));

        }catch(IOException e){
            e.printStackTrace();
            this.returnCode = JobState.FAILED;
        }
    }

    /**
     * A method to convert a String into a JSONObject
     * <p>
     * The method creates JSONObject based on StringBuffer. It will append lines from
     * the JSON file into StringBuffer and attempt to form a JSONObject every append.
     * Once successful, it will obtain the data from the JSONObject and put it into arrays.
     *
     * @param tempLine a string containing parts or a whole json object
     * @return true or false depending on the success of conversion.
     */
    private boolean convertToJSONObject(String tempLine){
        try {
            JSONObject convertedObject = new JSONObject(tempLine);

//          Insert into fieldNames
            if(this.fieldName.isEmpty()){
                this.fieldName = new ArrayList<Object>(convertedObject.keySet());
            }

//          Mapping values to keys
            ArrayList<Object> content = new ArrayList<Object>();
            for(Object element : this.fieldName){
                content.add(convertedObject.get((String)element));
            }
            this.rows.add(content);

            return true;

        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Parses through the XML file by chunks and stores the data in arrays.
     * <p>
     * The method uses XMLStreamReader to parse through the XML file.
     * It will go though a while loop until it reaches the end of the file
     * or the loop increments to the designated breakpoint. It goes through
     * a switch case to determine whether the tag is a closing or opening tag.
     * It will input the data from the tags into an array then call inputETLPacket.
     */
    private void extractXML(){

        int count=0;
        BufferedReader buff;
        RandomAccessFile raf = null;
        String startString="";
        boolean endReach = false;

        try{
            XMLInputFactory factory = XMLInputFactory.newInstance();
            raf = new RandomAccessFile(new File(this.filePath),"r");


            InputStream is = Channels.newInputStream(raf.getChannel());
            InputStreamReader isr = new InputStreamReader(is);
            buff = new BufferedReader(isr);
            XMLEventReader eventReader = factory.createXMLEventReader(buff);

            ArrayList<Object> tempField = new ArrayList<Object>();
            ArrayList<Object> tempRow = new ArrayList<Object>();


            while(eventReader.hasNext()&&count<docToRead){

                XMLEvent event = eventReader.nextEvent();

                switch(event.getEventType()){

                    //Since we cannot seek to position, we will loop through the document until it reaches to the bytePos
                    case XMLStreamConstants.START_DOCUMENT:
                        if(this.bytePos!=0){
                            startString=(String)this.etlPacket.get("recordTag");
                            while(event.getLocation().getCharacterOffset()+startString.getBytes().length<=this.bytePos){
                                event=eventReader.nextEvent();
                            }
                        }
                        break;
                    case XMLStreamConstants.START_ELEMENT:
                        //log.debug("XML Start Element");
                        StartElement startElement = event.asStartElement();
                        String qName = startElement.getName().getLocalPart();
                        //log.debug(qName);
                        if(this.etlPacket.has("rootTag")) {
                            if (startString.equals("")) {
                                startString = qName;
                                if(!this.etlPacket.has("recordTag")) {
                                    this.etlPacket.put("recordTag", qName);
                                }
                                break;
                            }
                            if(qName.equals(startString)){
                                break;
                            }
                            tempField.add(qName);
                        }else{
                            this.etlPacket.put("rootTag", qName);


                        }

                        break;
                    case XMLStreamConstants.CHARACTERS:
                        Characters characters = event.asCharacters();
                        if(characters.getData().contains("\n")){
                            break;
                        }
                        tempRow.add(characters.getData());
                        break;
                    case XMLStreamConstants.END_ELEMENT:
                        EndElement endElement = event.asEndElement();
                        String eName = endElement.getName().getLocalPart();
                        if(eName.equals(startString)) {
                            count++;

                            if (this.fieldName.isEmpty()) {
                                this.fieldName = tempField;
                            }
                            this.rows.add(tempRow);

                            tempField = new ArrayList<Object>();
                            tempRow = new ArrayList<Object>();

                        }else if(eName.equals(this.etlPacket.get("rootTag"))){
                            endReach = true;

                        }

                        break;
                    default:
                        break;
                }
                this.lastBytePos = event.getLocation().getCharacterOffset();

                //getCharacterOffset sets to next element
                //Adding offset of known element to lastBytePos
                if(endReach||count>=docToRead){
                    startString+="<>/";
                    this.lastBytePos = event.getLocation().getCharacterOffset()+startString.getBytes().length;
                    break;
                }
            }

            eventReader.close();
            inputETLPacket(); //Puts information to ETLPacket
            this.etlPacket.put("current_byte_position", (this.lastBytePos));

        }catch(FileNotFoundException e){
            e.printStackTrace();
            this.returnCode=JobState.FAILED;
        }catch (XMLStreamException e){
            e.printStackTrace();
            this.returnCode=JobState.FAILED;
        }catch (IOException e){
            e.printStackTrace();
            this.returnCode=JobState.FAILED;
        }

    }

    /**
     * Inputs the data stored into the arrays into ETLPacket
     * <p>
     * Creates a JSONObject and puts in the source header and JSONArray contents into it.
     * It will only put the source header once after it has been filled.
     */
    private void inputETLPacket(){

        JSONObject data = this.etlPacket.getJSONObject("data");
        JSONArray contents = data.getJSONArray("contents");

        for(List<Object> listString : this.rows){
            contents.put(listString);
        }

        if(etlPacket.getJSONObject("data").get("source_header").equals("")) {
            this.etlPacket.getJSONObject("data").put("source_header", this.fieldName);
        }

        data.put("contents",contents);
    }

//  Debugging purposes
    private void readContent(){
        log.debug("READCONTENT - ExtractTask - ETLPacket:\n"+this.etlPacket+"\n");
    }
}
