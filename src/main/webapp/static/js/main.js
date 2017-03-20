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
                             <input type="text" class="form-control" name="transformSourceField{0}" placeholder="Source Field"> \
                             <select class="form-control" name="transformValueComp{0}"> \
                                <option value="mult">MULT</option> \
                                <option value="div">DIV</option> \
                                <option value="pow">POW</option> \
                                <option value="add">ADD</option> \
                                <option value="sub">SUB</option> \
                                <option value="hash">HASH</option> \
                             </select> \
                             <input type="text" class="form-control" name="transformValue{0}" placeholder="Value"> \
                             <input type="text" class="form-control" name="transformDestinationField{0}" placeholder="New Field (optional)"> \
                             <button type="button" class="glyphicon glyphicon-remove-circle" id="transformCancel{0}"></button> \
                             <br> \
                         </div>';

    var mappingHtml = '<div class="form-group mappingGroup" id="mappingGroup{0}"> \
                           <input type="text" class="form-control" name="mappingSourceField{0}" placeholder="Source Field"> \
                           <input type="text" class="form-control" name="mappingDestinationField{0}" placeholder="Destination Field"> \
                           <button type="button" class="glyphicon glyphicon-remove-circle" id="mappingCancel{0}"></button> \
                           <br> \
                       </div>';

    var filterHtml = '<div class="form-group filterGroup" id="filterGroup{0}"> \
                        <input type="text" class="form-control" name="filterSourceField{0}" placeholder="Source Field"> \
                        <select class="form-control" name="filterValueComp{0}"> \
                            <option value="eq">==</option> \
                            <option value="gt">></option> \
                            <option value="lt"><</option> \
                        </select> \
                        <input type="text" class="form-control" name="filterValue{0}" placeholder="Value"> \
                        <button type="button" class="glyphicon glyphicon-remove-circle" id="filterCancel{0}"></button> \
                        <br> \
                      </div>';


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
                                alert(result);
                             },
                    data: $("#createWorkflowForm").serialize(),
                    type: "POST"
               });
    });

    $('#inProgressButton').click(function () {
        if ($('#inProgressButton').hasClass('active')) {
            $.ajax({
                        url: "/DataMETL/getworkflows",
                        success: function(result){
                                    alert(result);
                                 },
                        data: {'status': 'RUNNING'},
                        type: "GET"
                   });
        }
    });

    $('#completeButton').click(function () {
            if ($('#completeButton').hasClass('active')) {
                $.ajax({
                            url: "/DataMETL/getworkflows",
                            success: function(result){
                                        alert(result);
                                     },
                            data: {'status': 'SUCCESS'},
                            type: "GET"
                       });
            }
    });

    $('#failedButton').click(function () {
                if ($('#failedButton').hasClass('active')) {
                    $.ajax({
                                url: "/DataMETL/getworkflows",
                                success: function(result){
                                            alert(result);
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

});