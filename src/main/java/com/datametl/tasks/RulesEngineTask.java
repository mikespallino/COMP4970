package com.datametl.tasks;

import com.datametl.jobcontrol.JobState;
import com.datametl.jobcontrol.SubJob;
import com.datametl.logging.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

/**
 * Created by smithm19 on 2/6/17.
 *
 * RulesEngineTask performs the manipulation of data line by line according to the rules section of the ETL Packet.
 */
public class RulesEngineTask implements Task {

    private JobState current_state = JobState.RUNNING;
    private SubJob parent = null;
    private JSONArray newHeader;
    private JSONArray destinationHeader;
    private JSONArray sourceHeader;
    private Logger log;

    public RulesEngineTask(Logger log) {
        this.log = log;
    }


    /**
     * Gets ETL Packet from the parent process then applies filters to the current, transforms and mappings. (In that order)
     * Filter: loops through all filters within the ETL Packet and if the filter is false the line gets queued for deletion
     * Transform: loops through all transforms and applies current transform to the current single line. If the new_field field is null
     * then the transform is applied to the source_column, else a new field will be created with the value given in new_field.
     * Mapping: Reorder the modified line to match the destination_header by trimming out fields that aren't in the list of destination_headers
     *
     *
     * @see        Task
     */
    public void apply() {

        JSONObject pckt = parent.getETLPacket();

        JSONObject rules = pckt.getJSONObject("rules");
        JSONObject transforms = rules.getJSONObject("transformations");
        JSONObject mappings = rules.getJSONObject("mappings");
        JSONObject filters = rules.getJSONObject("filters");
        JSONObject packetData = pckt.getJSONObject("data");
        JSONArray dataContents = packetData.getJSONArray("contents");
        this.sourceHeader = packetData.getJSONArray("source_header");
        this.destinationHeader = packetData.getJSONArray("destination_header");
        this.newHeader = sourceHeader;
        List<Integer> toDeleteList = new ArrayList<Integer>();


        makeNewHeader(transforms, mappings);

        JSONArray headersToKeep = headerIndexesToKeep();
        //System.out.println(headersToKeep);

        //loop through each line
        for (int x =0; x<dataContents.length(); x++){
            JSONArray line = getLine(dataContents, x);
            log.debug("New Header: " + newHeader);
            log.debug("Pre Transform: " + line);
            if (!doFilters(line,filters)){
                toDeleteList.add(toDeleteList.size(), x);
            }else {
                line = doTransformations(transforms, line);
                log.debug("Pre Mapping: " + line);
                line = doMappings(mappings, line, headersToKeep);
                log.debug("Current Line: " + line);
                dataContents.put(x, line);
            }

        }
        deleteUnwantedElements(dataContents, toDeleteList);
        log.debug("THIS IS DATACONTENTS: " + dataContents);
        pckt.getJSONObject("data").put("contents", dataContents);
        log.debug(pckt.toString());
        log.debug("POST-Size of Data: " + dataContents.length());

        current_state = JobState.SUCCESS;
    }


    public JobState getResult() {

        return current_state;

    }

    public void setParent(SubJob parent) {
        this.parent = parent;
    }

    public SubJob getParent() {
        return this.parent;
    }

    private JSONArray deleteUnwantedElements(JSONArray data, List<Integer> listToDelete){
        if(listToDelete.size()==0){
            return data;
        }
        for (int x=listToDelete.size()-1;x>-1;x--){
            data.remove(listToDelete.get(x));

        }
        return data;
    }


    private JSONArray getLine(JSONArray content, int line_num) {
        return content.getJSONArray(line_num);
    }

    private static String hashString(String string, String typeOfHash){
        try {
            MessageDigest digest = MessageDigest.getInstance(typeOfHash);
            byte[] hashedBytes = digest.digest(string.getBytes("UTF-8"));

            return convertToHexString(hashedBytes);
        } catch (NoSuchAlgorithmException ex) {
            return "Could not generate hash from String" + ex;
        } catch (UnsupportedEncodingException ex){
            return "Could not generate hash from String" + ex;
        }
    }

    private static String convertToHexString(byte[] arrayBytes) {
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < arrayBytes.length; i++) {
            stringBuffer.append(Integer.toString((arrayBytes[i] & 0xff) + 0x100, 16)
                    .substring(1));
        }
        return stringBuffer.toString();
    }

    private JSONArray doMappings(JSONObject mappings, JSONArray line, JSONArray toKeep) {
        JSONArray sendback = new JSONArray();
        for (int x=0; x<destinationHeader.length();x++){
            sendback.put(x,JSONObject.NULL);
        }
        log.debug("1 THIS IS SENDBACK: " + sendback);
        for (int x =0; x<toKeep.length();x++){
            String headerName = newHeader.getString(toKeep.getInt(x));
            int headerIndex = getArrayIndex(destinationHeader, headerName);
            sendback.put(headerIndex, line.get(toKeep.getInt(x)));
        }

        log.debug("2 THIS IS SENDBACK: " + sendback);
        Iterator<?> keys = mappings.keys();
        while (keys.hasNext()){
            String curMapping = (String)keys.next();
            String newField = getCurrMappingDestinationField(mappings, curMapping);
            int indexCurMapping = getArrayIndex(newHeader,curMapping);
            int newFieldIndex = getArrayIndex(destinationHeader, newField);
            sendback.put(newFieldIndex, line.get(indexCurMapping));
        }
        log.debug("3 THIS IS SENDBACK: " + sendback);

        return sendback;
    }

    private JSONArray doTransformations(JSONObject transforms,JSONArray line) {
        //loop through all transformations for a single line
        for (int x=1; x<transforms.length()+1;x++) {
            String curTransform = "transform" + x;
            String newField = getCurrTransformNewField(transforms, curTransform);
            int indexToGet;
            String value;
            String transformValue;
            String curSource;



            //can be MULT, DIV, POW, ADD, SUB
            if (getCurrTransformSymbol(transforms, curTransform).equals("MULT")) {
                if(newField.equals("")) {
                    transformValue = getCurrTransformValue(transforms,curTransform);
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet  = getArrayIndex(this.sourceHeader,curSource);
                    value = line.get(indexToGet).toString();

                    line.put(indexToGet, Double.parseDouble(transformValue) * Double.parseDouble(value));

                }else{
                    transformValue = getCurrTransformValue(transforms,curTransform);
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet= getArrayIndex(this.newHeader,newField);
                    value = line.get(getArrayIndex(sourceHeader,curSource)).toString();

                    line.put(indexToGet,Double.parseDouble(transformValue) * Double.parseDouble(value));
                }
            }
            else if (getCurrTransformSymbol(transforms, curTransform).equals("DIV")) {
                if(newField.equals("")) {
                    transformValue = getCurrTransformValue(transforms,curTransform);
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet  = getArrayIndex(this.sourceHeader,curSource);
                    value = line.get(indexToGet).toString();

                    line.put(indexToGet, Double.parseDouble(transformValue) / Double.parseDouble(value));
                }else{
                    transformValue = getCurrTransformValue(transforms,curTransform);
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet= getArrayIndex(this.newHeader,newField);
                    value = line.get(getArrayIndex(sourceHeader,curSource)).toString();

                    line.put(indexToGet,Double.parseDouble(transformValue) / Double.parseDouble(value));
                }
            }
            else if (getCurrTransformSymbol(transforms, curTransform).equals("ADD")) {
                if(newField.equals("")) {
                    transformValue = getCurrTransformValue(transforms,curTransform);
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet  = getArrayIndex(this.sourceHeader,curSource);
                    value = line.get(indexToGet).toString();

                    line.put(indexToGet, Double.parseDouble(transformValue) + Double.parseDouble(value));
                }else{
                    transformValue = getCurrTransformValue(transforms,curTransform);
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet= getArrayIndex(this.newHeader,newField);
                    value = line.get(getArrayIndex(sourceHeader,curSource)).toString();

                    line.put(indexToGet,Double.parseDouble(transformValue) + Double.parseDouble(value));
                }
            }
            else if (getCurrTransformSymbol(transforms, curTransform).equals("SUB")) {
                if(newField.equals("")) {
                    transformValue = getCurrTransformValue(transforms,curTransform);
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet  = getArrayIndex(this.sourceHeader,curSource);
                    value = line.get(indexToGet).toString();

                    line.put(indexToGet, Double.parseDouble(transformValue) - Double.parseDouble(value));
                }else{
                    transformValue = getCurrTransformValue(transforms,curTransform);
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet= getArrayIndex(this.newHeader,newField);
                    value = line.get(getArrayIndex(sourceHeader,curSource)).toString();

                    line.put(indexToGet,Double.parseDouble(transformValue) - Double.parseDouble(value));
                }
            }
            else if (getCurrTransformSymbol(transforms, curTransform).equals("POW")) {
                if(newField.equals("")) {
                    transformValue = getCurrTransformValue(transforms,curTransform);
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet  = getArrayIndex(this.sourceHeader,curSource);
                    value = line.get(indexToGet).toString();

                    line.put(indexToGet, Math.pow(Double.parseDouble(transformValue), Double.parseDouble(value)));
                }else{
                    transformValue = getCurrTransformValue(transforms,curTransform);
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet= getArrayIndex(this.newHeader,newField);
                    value = line.get(getArrayIndex(sourceHeader,curSource)).toString();

                    line.put(indexToGet, Math.pow(Double.parseDouble(transformValue),Double.parseDouble(value)));
                }

            }
            else if (getCurrTransformSymbol(transforms,curTransform).equals("HASH")){

                if (newField.equals("")){
                    transformValue = getCurrTransformValue(transforms,curTransform);
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet= getArrayIndex(this.sourceHeader,curSource);
                    value = line.get(indexToGet).toString();

                    if (transformValue.equals("MD5")){
                        line.put(indexToGet,hashString(value, "MD5"));
                    }
                    else if (transformValue.equals("SHA1")){
                        line.put(indexToGet,hashString(value, "SHA-1"));
                    }
                    else if (transformValue.equals("SHA256")){
                        line.put(indexToGet,hashString(value, "SHA-256"));
                    }

                }
                else {
                    transformValue = getCurrTransformValue(transforms, curTransform);
                    curSource = getCurrTransformSource(transforms, curTransform);
                    indexToGet = getArrayIndex(this.newHeader, newField);
                    value = line.get(getArrayIndex(sourceHeader, curSource)).toString();
                    if (transformValue.equals("MD5")){
                        line.put(indexToGet,hashString(value, "MD5"));
                    }
                    else if (transformValue.equals("SHA1")){
                        line.put(indexToGet,hashString(value, "SHA-1"));
                    }
                    else if (transformValue.equals("SHA256")){
                        line.put(indexToGet,hashString(value, "SHA-256"));
                    }
                }
            }
            else if (getCurrTransformSymbol(transforms, curTransform).equals("UCASE")) {
                if(newField.equals("")) {
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet  = getArrayIndex(this.sourceHeader,curSource);

                    if (line.get(indexToGet) instanceof String) {
                        line.put(indexToGet, line.getString(indexToGet).toUpperCase());
                    }
                }else{
                    indexToGet= getArrayIndex(this.newHeader,newField);

                    if (line.get(indexToGet) instanceof String) {
                        line.put(indexToGet, line.getString(indexToGet).toUpperCase());
                    }
                }
            }
            else if (getCurrTransformSymbol(transforms, curTransform).equals("LCASE")) {
                if(newField.equals("")) {
                    curSource = getCurrTransformSource(transforms,curTransform);
                    indexToGet  = getArrayIndex(this.sourceHeader,curSource);

                    if (line.get(indexToGet) instanceof String) {
                        line.put(indexToGet, line.getString(indexToGet).toLowerCase());
                    }
                }else{
                    indexToGet= getArrayIndex(this.newHeader,newField);

                    if (line.get(indexToGet) instanceof String) {
                        line.put(indexToGet, line.getString(indexToGet).toLowerCase());
                    }
                }
            }
        }



        return line;
    }

    private Boolean doFilters(JSONArray line,JSONObject filters) {
        for (int x=1; x<filters.length()+1;x++) {
            String curFilter = "filter" + x;
            String curFilterColumn = filters.getJSONObject(curFilter).get("source_column").toString();
            int indexOfColumn = getArrayIndex(sourceHeader,curFilterColumn);
            //String curFilterValue = filters.getJSONObject(curFilter).get("filter_value").toString();
            String curFilterSymbol = filters.getJSONObject(curFilter).get("equality_test").toString();
            //String curFilterNumber = curFilterValue.split(" ")[1];

            if (curFilterSymbol.equals("EQ")) {
                if (filters.getJSONObject(curFilter).get("filter_value") instanceof String) {
                    if (filters.getJSONObject(curFilter).get("filter_value").equals(line.get(indexOfColumn))) {
                    }else{
                        return false;
                    }
                }
                else if (filters.getJSONObject(curFilter).get("filter_value") instanceof Double) {
                    if (filters.getJSONObject(curFilter).getDouble("filter_value") == line.getDouble(indexOfColumn)) {
                    }else{
                        return false;
                    }
                }
                else if (filters.getJSONObject(curFilter).get("filter_value") instanceof Integer) {
                    if (filters.getJSONObject(curFilter).getInt("filter_value") == Integer.parseInt(line.get(indexOfColumn).toString())) {
                    }else{
                        return false;
                    }
                }
                else if (filters.getJSONObject(curFilter).get("filter_value") instanceof Long) {
                    if (filters.getJSONObject(curFilter).getLong("filter_value") == Long.parseLong(line.get(indexOfColumn).toString())) {
                    }else{
                        return false;
                    }
                }
            }
            else if (curFilterSymbol.equals("GT")) {
                if (filters.getJSONObject(curFilter).get("filter_value") instanceof String) {
  /*                  if (filters.getJSONObject(curFilter).getString("filter_value") < line.getString(indexOfColumn)) {
                    }else{
                        return false;
                    }*/
                }
                else if (filters.getJSONObject(curFilter).get("filter_value") instanceof Double) {
                    if (filters.getJSONObject(curFilter).getDouble("filter_value") < line.getDouble(indexOfColumn)) {
                    }else{
                        return false;
                    }
                }
                else if (filters.getJSONObject(curFilter).get("filter_value") instanceof Integer) {
                    if (filters.getJSONObject(curFilter).getInt("filter_value") < Integer.parseInt(line.get(indexOfColumn).toString())) {
                    }else{
                        return false;
                    }
                }
                else if (filters.getJSONObject(curFilter).get("filter_value") instanceof Long) {
                    if (filters.getJSONObject(curFilter).getLong("filter_value") < Long.parseLong(line.get(indexOfColumn).toString())) {
                    }else{
                        return false;
                    }
                }
            }
            else if (curFilterSymbol.equals("LT")) {
                if (filters.getJSONObject(curFilter).get("filter_value") instanceof String) {
  /*                  if (filters.getJSONObject(curFilter).getString("filter_value") > line.getString(indexOfColumn)) {
                    }else{
                        return false;
                    }*/
                }
                else if (filters.getJSONObject(curFilter).get("filter_value") instanceof Double) {
                    if (filters.getJSONObject(curFilter).getDouble("filter_value") > line.getDouble(indexOfColumn)) {
                    }else{
                        return false;
                    }
                }
                else if (filters.getJSONObject(curFilter).get("filter_value") instanceof Integer) {
                    if (filters.getJSONObject(curFilter).getInt("filter_value") > Integer.parseInt(line.get(indexOfColumn).toString())) {
                    }else{
                        return false;
                    }
                }
                else if (filters.getJSONObject(curFilter).get("filter_value") instanceof Long) {
                    if (filters.getJSONObject(curFilter).getLong("filter_value") > Long.parseLong(line.get(indexOfColumn).toString())) {
                    }else{
                        return false;
                    }
                }

            }
        }


        return true;
    }

    private int getArrayIndex(JSONArray arr, String toCompare) {
        for (int i = 0; i < arr.length(); i++) {
            if (toCompare.equals(arr.get(i))) {
                return i;
            }
        }
        return -1;
    }


    private String getCurrMappingDestinationField(JSONObject mapping,String curMapping){
        return mapping.getString(curMapping);
    }

    //code looks like garbage with this stuff everywhere, so I made Transformation functions to limit the grossness
    ////
    ////
    private String getCurrTransformNewField(JSONObject trans,String curTransform){

        if (trans.getJSONObject(curTransform).get("new_field").equals(null)){
            return "";
        }
        return trans.getJSONObject(curTransform).getString("new_field");
    }

    private String getCurrTransformSource(JSONObject trans, String curTransform){
        return trans.getJSONObject(curTransform).getString("source_column");
    }

    private String getCurrTransformValue(JSONObject trans, String curTransform){
        return trans.getJSONObject(curTransform).getString("transform").split(" ")[1];
    }
    private String getCurrTransformSymbol(JSONObject trans,String curTransform){
        return trans.getJSONObject(curTransform).getString("transform").split(" ")[0];
    }
    //Makes newHeader
    private void makeNewHeader(JSONObject transforms, JSONObject mappings){
        for (int x=1; x<transforms.length()+1;x++) {
            String curTransform = "transform" + x;
            String newField = getCurrTransformNewField(transforms, curTransform);
            if (newField.equals("")) {

            } else if (checkHeaderDuplicate(newHeader, newField)) {
                newHeader.put(newHeader.length(), newField);
            }

        }



    }
    ////
    ////


    //returns an array of indexes to keep that match the newHeader to DestinationHeader
    private JSONArray headerIndexesToKeep(){
       JSONArray elementsToKeep = new JSONArray();

       for (int x=0; x<destinationHeader.length();x++){
           for(int y=0; y<newHeader.length();y++){
               if (newHeader.get(y).equals(destinationHeader.get(x))){
                   elementsToKeep.put(elementsToKeep.length(),getArrayIndex(newHeader,newHeader.get(y).toString()));
               }
           }
       }
       return elementsToKeep;
    }

    private boolean checkHeaderDuplicate(JSONArray heads, String word){
        for (int x=0; x<heads.length(); x++){
            if (heads.get(x).equals(word)){
                return false;
            }
        }
        return true;
    }
}

