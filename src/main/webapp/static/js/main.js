$(document).ready(function() {
    var transformCount = 0;
    var mappingCount = 0;
    var filterCount = 0;

    if (!String.prototype.format) {
      String.prototype.format = function() {
        var args = arguments;
        return this.replace(/{(\d+)}/g, function(match, number) {
          return typeof args[number] != 'undefined'
            ? args[number]
            : match
          ;
        });
      };
    }

    var transformHtml = '<div class="form-group transformGroup" id="transformGroup{0}"> \
                             <input type="text" class="form-control" name="transformSourceField{0}" id="transformSourceField{0}" placeholder="Source Field"> \
                             <select class="form-control" name="transformValueComp{0}" id="transformValueComp{0}"> \
                                <option value="mult">MULT</option> \
                                <option value="div">DIV</option> \
                                <option value="pow">POW</option> \
                                <option value="add">ADD</option> \
                                <option value="sub">SUB</option> \
                                <option value="hash">HASH</option> \
                             </select> \
                             <input type="text" class="form-control" name="transformValue{0}" id="transformValue{0}" placeholder="Value"> \
                             <input type="text" class="form-control" name="transformDestinationField{0}" id="transformDestinationField{0}" placeholder="New Field (optional)"> \
                             <button type="button" class="glyphicon glyphicon-remove-circle" id="transformCancel{0}"></button> \
                             <br> \
                         </div>';

    var mappingHtml = '<div class="form-group mappingGroup" id="mappingGroup{0}"> \
                           <input type="text" class="form-control" name="mappingSourceField{0}" id="mappingSourceField{0}" placeholder="Source Field"> \
                           <input type="text" class="form-control" name="mappingDestinationField{0}" id="mappingDestinationField{0}" placeholder="Destination Field"> \
                           <button type="button" class="glyphicon glyphicon-remove-circle" id="mappingCancel{0}"></button> \
                           <br> \
                       </div>';

    var filterHtml = '<div class="form-group filterGroup" id="filterGroup{0}"> \
                        <input type="text" class="form-control" name="filterSourceField{0}" id="filterSourceField{0}" placeholder="Source Field"> \
                        <select class="form-control" name="filterValueComp{0}" id="filterValueComp{0}"> \
                            <option value="eq">==</option> \
                            <option value="gt">></option> \
                            <option value="lt"><</option> \
                        </select> \
                        <input type="text" class="form-control" name="filterValue{0}" id="filterValue{0}" placeholder="Value"> \
                        <button type="button" class="glyphicon glyphicon-remove-circle" id="filterCancel{0}"></button> \
                        <br> \
                      </div>';

    var workflowHtml = '<div id="{0}"><button id="button-{0}"><h5>{0}</h5></button></div>';


    $('#transform-new').click(function () {
        $(transformHtml.format(transformCount)).appendTo('#transforms');

        makeTransformCancelButtonFunction(transformCount);
        transformCount++;
    });

    $('#mapping-new').click(function () {
        $(mappingHtml.format(mappingCount)).appendTo('#mapping');
        makeMappingCancelButtonFunction(mappingCount);
        mappingCount++;
    });

    $('#filter-new').click(function () {
        $(filterHtml.format(filterCount)).appendTo('#filters');
        makeFilterCancelButtonFunction(filterCount);
        filterCount++;
    });

    $('#executeButton').click(function () {
        $.ajax({
                    url: "/DataMETL/createworkflow",
                    success: function(result){
                                alert("Job is now running!");
                             },
                    error: function(XMLHttpRequest, textStatus, errorThrown) {
                            alert("ERROR: A Job with than name already exists!");
                           },
                    data: $("#createWorkflowForm").serialize(),
                    type: "POST"
               });
    });

    $('#createButton').click(function () {
        $('#datametl-page').load('workflow_template.html');
    });

    $('#inProgressButton').click(function () {
        if ($('#inProgressButton').hasClass('active')) {
            $('#datametl-page').html("");
            $.ajax({
                        url: "/DataMETL/getworkflows",
                        success: function(result){
                                    parseWorkflowResponse(result, '#inProgressLocation', 'In Progress Workflow', true);
                                 },
                        data: {'status': 'RUNNING'},
                        type: "GET"
                   });
        }
    });

    $('#completeButton').click(function () {
        if ($('#completeButton').hasClass('active')) {
            $('#datametl-page').html("");
            $.ajax({
                        url: "/DataMETL/getworkflows",
                        success: function(result){
                                    parseWorkflowResponse(result, '#completeLocation', 'Completed Workflow', false);
                                 },
                        data: {'status': 'SUCCESS'},
                        type: "GET"
                   });
        }
    });

    $('#failedButton').click(function () {
        if ($('#failedButton').hasClass('active')) {
            $('#datametl-page').html("");
            $.ajax({
                        url: "/DataMETL/getworkflows",
                        success: function(result){
                                    parseWorkflowResponse(result, '#failedLocation', 'Failed Workflow', false);
                                 },
                        data: {'status': 'FAILED'},
                        type: "GET"
                   });
        }
        });

    function makeTransformCancelButtonFunction(count) {
        $('#transformCancel' + count).click(function () {
            $('#transformGroup' + count).remove();
        });
    }

    function makeMappingCancelButtonFunction(count) {
        $('#mappingCancel' + count).click(function () {
            $('#mappingGroup' + count).remove();
        });
    }

    function makeFilterCancelButtonFunction(count) {
            $('#filterCancel' + count).click(function () {
                $('#filterGroup' + count).remove();
            });
    }

    function parseWorkflowResponse(result, divid, title, shouldMakeCancelButton) {
        var response = JSON.parse(result);
        var key;
        // Empty out the buttons every time
        $(divid).html('');
        for(key in response){
            if(document.getElementById(key) == null) {
                $(workflowHtml.format(key)).appendTo(divid);
                makeWorkflowInfoButton(key, title, shouldMakeCancelButton);
            }
        }
    }

    function makeWorkflowInfoButton(jobid, title, shouldMakeCancelButton) {
        var transformPacketCount = 0;
        var mappingPacketCount = 0;
        var filterPacketCount = 0;

        $('#button-' + jobid).click(function() {
        $.ajax({
                url: "/DataMETL/getworkflows?jobid={0}".format(jobid),
                success: function(result){
                            $('#datametl-page').html("");
                            $('#datametl-page').load('workflow_template.html', function() {
                                if(shouldMakeCancelButton == true) {
                                    $('<div style="margin: auto;width: 50%;"><h3>{0}</h3></div><button type="button" class="btn btn-danger" id="cancelButton" style="float:right;">Cancel</button><br/><br/><br/>'.format(title, jobid)).insertBefore('#createWorkflowForm');
                                    $('#cancelButton').click(function() {
                                        $.ajax({
                                                    url: "cancelworkflow?jobid={0}".format(jobid),
                                                    success: function(result){
                                                                $('#datametl-page').html("");
                                                             },
                                                    type: 'GET'
                                                });
                                    });
                                } else {
                                    $('<div style="margin: auto;width: 50%;"><h3>{0}</h3></div><br/><br/><br/>'.format(title)).insertBefore('#createWorkflowForm');
                                }
                                $('#executeButton').prop('disabled', true);
                                $('#transform-new').prop('disabled', true);
                                $('#mapping-new').prop('disabled', true);
                                $('#filter-new').prop('disabled', true);

                                var response = JSON.parse(result);
                                var source = response['source'];
                                var rules = response['rules'];
                                var destination = response['destination'];
                                var data = response['data'];
                                var key;
                                for(key in rules['transformations']){
                                    $(transformHtml.format(transformPacketCount)).appendTo('#transforms');
                                    makeTransformCancelButtonFunction(transformPacketCount);

                                    $('#transformSourceField'+transformPacketCount).val(rules['transformations'][key]['source_column']);
                                    $('#transformDestinationField'+transformPacketCount).val(rules['transformations'][key]['new_field']);
                                    $('#transformValueComp'+transformPacketCount).val(rules['transformations'][key]['transform'].split(' ')[0].toLowerCase());
                                    $('#transformValue'+transformPacketCount).val(rules['transformations'][key]['transform'].split(' ')[1]);
                                    $('#transformGroup'+transformPacketCount).find('*').prop('disabled', true);
                                    transformPacketCount++;
                                }

                                for(key in rules['mappings']){
                                    $(mappingHtml.format(mappingPacketCount)).appendTo('#mapping');
                                    makeMappingCancelButtonFunction(mappingPacketCount);2

                                    $('#mappingSourceField'+mappingPacketCount).val(key);
                                    $('#mappingDestinationField'+mappingPacketCount).val(rules['mappings'][key]);
                                    $('#mappingGroup'+mappingPacketCount).find('*').prop('disabled', true);
                                    mappingPacketCount++;
                                }

                                for(key in rules['filters']){
                                    $(filterHtml.format(filterPacketCount)).appendTo('#filters');
                                    makeFilterCancelButtonFunction(filterPacketCount);

                                    $('#filterSourceField'+filterPacketCount).val(rules['filters'][key]['source_column']);
                                    $('#filterValueComp'+filterPacketCount).val(rules['filters'][key]['equality_test'].toLowerCase());
                                    $('#filterValue'+filterPacketCount).val(rules['filters'][key]['filter_value']);
                                    $('#filterGroup'+filterPacketCount).find('*').prop('disabled', true);
                                    filterPacketCount++;
                                }

                                $('#source-config').find('*').prop('disabled', true);
                                $('#destination-config').find('*').prop('disabled', true);
                                $('#execution-config').find('*').prop('disabled', true);

                                $('#source').val(source['path']);
                                $('#source_type').val(source['file_type'].toLowerCase());
                                $('#destination_type').val(destination['storage_type']);
                                $('#destination_port').val(destination['host_port']);
                                $('#destination_ip').val(destination['host_port']);
                                //TODO: Once Zach is done.
                                $('#destination_location').val("NONE");
                                $('#destination_schema').val(data['destination_header']);
                                $('#username').val(destination['username']);
                                $('#password').val(destination['password']);
                                $('#time').val(response['time']);
                                $('#schedule').val(response['schedule']);
                                $('#name').val(response['name']);
                            });
                         },
                type: "GET"
           });
        })
    }

});