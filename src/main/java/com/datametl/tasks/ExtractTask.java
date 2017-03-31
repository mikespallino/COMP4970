package com.datametl.tasks;

import com.datametl.exception.JSONParsingException;
import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
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

    public ExtractTask() {
    }

    public void apply() {
        this.returnCode = JobState.RUNNING;
        this.etlPacket = parent.getETLPacket();
        this.filePath = etlPacket.getJSONObject("source").getString("path");
        this.fileType = etlPacket.getJSONObject("source").getString("file_type");
        this.docToRead = etlPacket.getInt("documents_to_read");
        this.bytePos = etlPacket.getLong("current_byte_position");

        if(this.fileType.equals("csv")){
            System.out.println("PARSING AT CSV AT: "+this.bytePos);
            extractCSV();
            System.out.println("Byte Location: "+this.lastBytePos);
        }else if(this.fileType.equals("json")){
            try {
                System.out.println("PARSING AT JSON AT: "+this.bytePos);
                extractJSON();
                System.out.println("Byte Location: "+this.lastBytePos);
            }catch(JSONParsingException e){
                e.printStackTrace();
            }
        }else if(this.fileType.equals("xml")){
            System.out.println("PARSING AT XML AT: "+this.bytePos);
            extractXML();
            System.out.println("Byte Location: "+this.lastBytePos);
        }else{
            this.returnCode = JobState.FAILED;
        }


//        Task rules = new RulesEngineTask();
//        SubJob newRulesSubJob = new SubJob(rules);

//      INFO: Give RulesEngine a copy and reset the contents
//        newRulesSubJob.setETLPacket(new JSONObject(etlPacket.toString()));
        JSONArray empty = new JSONArray();
        etlPacket.getJSONObject("data").put("contents", empty);

//        boolean status = parent.getParent().addSubJob(newRulesSubJob);

        returnCode = JobState.SUCCESS;

    }

    public JobState getResult() {
        return this.returnCode;
    }

    public void setParent(SubJob parent) {
        this.parent = parent;
    }

    public SubJob getParent() {
        return parent;
    }

    private void extractCSV() {

        int count=0;
        int linesToSkip=0;
        BufferedReader buff;
        RandomAccessFile raf = null;

        if(this.etlPacket.has("linesRead")){
            linesToSkip = (Integer)this.etlPacket.get("linesRead");
        }else{
            this.etlPacket.put("linesRead",0);
        }

        try{

            raf = new RandomAccessFile(new File(this.filePath),"r");

           //raf.seek(bytePos);

            InputStream is = Channels.newInputStream(raf.getChannel());
            InputStreamReader isr = new InputStreamReader(is);
            buff = new BufferedReader(isr);
            CSVReader reader = new CSVReader(buff, CSVParser.DEFAULT_SEPARATOR,CSVParser.DEFAULT_QUOTE_CHARACTER,linesToSkip);
            Object[] nextLine;

            //Each nextLine is a row in CSV
            while((nextLine=reader.readNext())!=null){
                if(nextLine!=null) {
                    List<Object> listLine = Arrays.asList(nextLine);
                    if (this.fieldName.isEmpty()) {
                        //First line must be field names
                        this.fieldName = new ArrayList<Object>(listLine);
                    } else {

                        this.rows.add(listLine);
                        //System.out.println(listLine);
                    }
                }
//                this.lastBytePos = raf.getFilePointer();
//                System.out.println("Byte Location: "+this.lastBytePos + "-------------- Count: "+count);
                count++;
                if(count>=this.docToRead){
                    break;
                }
                //System.out.println(count);

            }

            //System.out.println("LINES READ: "+reader.getLinesRead());
            this.lastBytePos = raf.getFilePointer();

            reader.close();
            inputETLPacket(); //Puts information to ETLPacket
            this.etlPacket.put("current_byte_position", (this.lastBytePos));
            this.etlPacket.put("linesRead",linesToSkip+count);

            //readContent();
        }catch(IOException e){
            e.printStackTrace();
            this.returnCode = JobState.FAILED;
        }

        //this.etlPacket.put("current_byte_position", (lastBytePos-bytePos)+bytePos);


    }

    private void extractJSON() throws JSONParsingException {

        int count = 0;
        int breakCount=0;
        int maxBreakCount=500;

        try{


            RandomAccessFile randomAccessFile = new RandomAccessFile(this.filePath,"r");

            StringBuffer stringBuffer = new StringBuffer();
            String line;

            if(this.bytePos==0){
                line = randomAccessFile.readLine();
            }else{
                randomAccessFile.seek(bytePos);
                line = randomAccessFile.readLine();
            }
            //String line = randomAccessFile.readLine();

            while(breakCount<maxBreakCount) {
                boolean convertSuccess=false;

                if(randomAccessFile.getFilePointer()>= randomAccessFile.length()){
                    this.lastBytePos=randomAccessFile.getFilePointer();
                    this.etlPacket.put("current_byte_position",this.lastBytePos);
                    System.out.println("END OF FILE - CLOSING");
                    randomAccessFile.close();
                    break;
                }

                this.lastBytePos = randomAccessFile.getFilePointer();
//                System.out.println("Byte Location 1 : "+this.lastBytePos + "-------------- Count: "+count);

                //If first line has '[', ignore and add to stringBuffer
                if (line.charAt(0) == '[') {

                    //Appends line without [
                    stringBuffer.append(line.substring(1));
                    line = line.substring(1);
                    String tempLine="";

                    if (stringBuffer.charAt(stringBuffer.length() - 1) == ',') {
                        tempLine = stringBuffer.substring(0, stringBuffer.length() - 1);
                        convertSuccess=convertToJSONObject(tempLine);

                    }
                    else if(stringBuffer.charAt(stringBuffer.length() - 1) == ']') {
                        tempLine = stringBuffer.substring(0, stringBuffer.length() - 1);
                        convertSuccess=convertToJSONObject(tempLine);
                    }
//                    else if(stringBuffer.charAt(stringBuffer.length() - 1) == '}'){
//                        tempLine = stringBuffer.substring(0, stringBuffer.length() - 1);
//                        convertSuccess=convertToJSONObject(tempLine);
//                    }

                    if(convertSuccess){
                        count++;
                        stringBuffer.setLength(0);
                    }

                } else {
                    //If not the first line, append
                    stringBuffer.append(randomAccessFile.readLine());
                    String tempLine="";

                    if (stringBuffer.charAt(stringBuffer.length() - 1) == ',') {
                        tempLine = stringBuffer.substring(0, stringBuffer.length() - 1);
                        convertSuccess=convertToJSONObject(tempLine);
                    }
                    else if(stringBuffer.charAt(stringBuffer.length() - 1) == ']') {
                        tempLine = stringBuffer.substring(0, stringBuffer.length() - 1);
                        convertSuccess=convertToJSONObject(tempLine);
                    }
//                    else if(stringBuffer.charAt(stringBuffer.length() - 1) == '}'){
//                        tempLine = stringBuffer.substring(0, stringBuffer.length() - 1);
//                        convertSuccess=convertToJSONObject(tempLine);
//                    }

                    if(convertSuccess){
                        count++;
                        stringBuffer.setLength(0);
                    }

                }
//                System.out.println("\n-------------------A Line---------------------");
//                System.out.println("Line: " + stringBuffer);
//                System.out.println("Byte location: " + randomAccessFile.getFilePointer());
//                System.out.println("Count: "+count);

                if(count>=this.docToRead){
                    System.out.println("Byte Location SEIZE : "+this.lastBytePos + "-------------- Count: "+count);
                    break;
                }else{
                    this.lastBytePos = randomAccessFile.getFilePointer();
//                    System.out.println("Byte Location 2 : "+this.lastBytePos + "-------------- Count: "+count);
                }

                //Increment counter to determine if valid
                breakCount++;



            }

            if(breakCount>=maxBreakCount){
                randomAccessFile.close();
                this.returnCode = JobState.FAILED;
                throw new JSONParsingException("Parsing JSONObject timed out. File is either invalid, formatted incorrectly or the object is too large.");
            }else{
                randomAccessFile.close();
                inputETLPacket();
            }

            this.etlPacket.put("current_byte_position", (this.lastBytePos));
            //readContent();

        }catch(IOException e){
            e.printStackTrace();
            this.returnCode = JobState.FAILED;
        }
    }

    private boolean convertToJSONObject(String tempLine){
        try {
            //System.out.println("\n-----------------------Convert JSON with Bracket--------------------");
            //System.out.println(tempLine);
            JSONObject convertedObject = new JSONObject(tempLine);
            //System.out.println("Successful in converting");

            //Insert into fieldNames
            if(this.fieldName.isEmpty()){
                this.fieldName = new ArrayList<Object>(convertedObject.keySet());
            }

            //Mapping values to keys
            ArrayList<Object> content = new ArrayList<Object>();
            for(Object element : this.fieldName){
                //System.out.println("VALUE: "+convertedObject.get((String)element));
                content.add(convertedObject.get((String)element));
            }
            this.rows.add(content);


            //System.out.println("Successful in insertion");
            return true;

        } catch (Exception e) {
            //System.out.println("Failed to convert/insert");
            return false;
        }
    }

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

//            byte[] something =new byte[(int) 150];
//            try {
//                raf.read(something);
//            }catch(Exception ee){
//            }
//            System.out.println("SHIT - "+ new String(something));
//            raf.seek(this.lastBytePos);
//
//            if(this.bytePos!=0){
//                raf.seek(this.bytePos);
//            }

            ArrayList<Object> tempField = new ArrayList<Object>();
            ArrayList<Object> tempRow = new ArrayList<Object>();


            while(eventReader.hasNext()&&count<docToRead){

                XMLEvent event = eventReader.nextEvent();

                //System.out.println("KEEP PRINTING: "+raf.getFilePointer());
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
                        //System.out.println("XML Start Element");
                        StartElement startElement = event.asStartElement();
                        String qName = startElement.getName().getLocalPart();
                        //System.out.println(qName);
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
                        //System.out.println("XML Character");
                        Characters characters = event.asCharacters();
                        //System.out.println("Characters Data: "+characters.getData());
                        if(characters.getData().contains("\n")){
                            break;
                        }
                        tempRow.add(characters.getData());
                        break;
                    case XMLStreamConstants.END_ELEMENT:
                        //System.out.println("XML End Element");
                        EndElement endElement = event.asEndElement();
                        String eName = endElement.getName().getLocalPart();
                        //System.out.println("End Element: "+eName);
                        //System.out.println("Root End: "+this.etlPacket.get("rootTag"));
                        if(eName.equals(startString)) {
                            count++;

                            if (this.fieldName.isEmpty()) {
                                this.fieldName = tempField;
                            }
                            this.rows.add(tempRow);

                            tempField = new ArrayList<Object>();
                            tempRow = new ArrayList<Object>();

                            //System.out.println("Field Name: " + this.fieldName);
                            //System.out.println("Rows: " + this.rows);
                        }else if(eName.equals(this.etlPacket.get("rootTag"))){
                            endReach = true;

                        }

                        break;
                    default:
                        //System.out.println(event.getEventType());
                        break;
                }
                //System.out.println("Start String: "+startString);
                //System.out.println("Temp Field Name: " + tempField);
                //System.out.println("Temp Rows: " + tempRow);
                this.lastBytePos = event.getLocation().getCharacterOffset();

                //getCharacterOffset sets to next element
                //Adding offset of known element to lastBytePos
                if(endReach||count>=docToRead){
                    //System.out.println("\n\n\n\n--------------------------I REACHED HERE--------------------------\n\n\n\n");
                    startString+="<>/";
                    this.lastBytePos = event.getLocation().getCharacterOffset()+startString.getBytes().length;
                    break;
                }
            }

//            System.out.println(this.lastBytePos);
//            byte[] something =new byte[(int) 150];
//            try {
//                raf.read(something);
//            }catch(Exception ee){
//            }
//            System.out.println("SHIT - "+ new String(something));
//            raf.seek(this.lastBytePos);

            eventReader.close();
            inputETLPacket(); //Puts information to ETLPacket
            this.etlPacket.put("current_byte_position", (this.lastBytePos));

        }catch(FileNotFoundException e){
            e.printStackTrace();
        }catch (XMLStreamException e){
            byte[] something =new byte[(int) 150];
            try {
                raf.read(something);
            }catch(Exception ee){
            }
            //System.out.println("Something - "+ new String(something));
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }

    }

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
//        this.etlPacket.put("data",data);

        //System.out.println("INPUT - ExtractTask - ETLPacket:\n"+this.etlPacket+"\n");
        readContent();
    }

    private void readContent(){
        //System.out.println("READCONTENT - fieldName: "+this.fieldName);

        System.out.println("READCONTENT - ExtractTask - ETLPacket:\n"+this.etlPacket+"\n");
    }
}
