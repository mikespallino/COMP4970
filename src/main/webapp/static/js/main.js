$(document).ready(function() {
    var transformCount = 0;
    var mappingCount = 0;
    var filterCount = 0;
    $('#transform-new').click(function () {
        var addContent = "<div class='form-group'>";
        addContent += "<input type='text' class='form-control' id='transformSourceField" + transformCount + "' name='transformSourceField" + transformCount + "' placeholder='Source Field'>";
        addContent += "<select class='form-control' id='transformValueComp" + transformCount + "' name='transformValueComp" + transformCount + "'>";
        addContent += "<option value='mult'>MULT</option>";
        addContent += "<option value='div'>DIV</option>";
        addContent += "<option value='pow'>POW</option>";
        addContent += "<option value='add'>ADD</option>";
        addContent += "<option value='sub'>SUB</option>";
        addContent += "<option value='hash'>HASH</option>";
        addContent +=  "</select>";
        addContent += "<input type='text' class='form-control' id='transformValue" + transformCount + "' name='transformValue" + transformCount + "' placeholder='Value'>";
        addContent += "<input type='text' class='form-control' id='transformDestinationField" + transformCount + "' name='transformDestinationField" + transformCount + "' placeholder='New Field (optional)'>";
        addContent += "<button type='button' class='glyphicon glyphicon-remove-circle' id='transformCancel" + transformCount + "'></button";
        addContent += "</div>";
        addContent += "<br>";

        $(addContent).appendTo('#transforms');

        makeTransformCancelButtonFunction(transformCount);
        transformCount++;
    });

    $('#mapping-new').click(function () {
        var addContent = "<div class='form-group'>";
        addContent += "<input type='text' class='form-control' id='mappingSourceField" + mappingCount + "' name='mappingSourceField" + mappingCount + "' placeholder='Source Field'>";
        addContent += "<input type='text' class='form-control' id='mappingDestinationField" + mappingCount + "' name='mappingDestinationField" + mappingCount + "' placeholder='Destination Field'>";
        addContent += "<button type='button' class='glyphicon glyphicon-remove-circle' id='mappingCancel" + mappingCount + "'></button";
        addContent += "</div>";
        addContent += "<br>";

        $(addContent).appendTo('#mapping');
        makeMappingCancelButtonFunction(mappingCount);
        mappingCount++;
    });

    $('#filter-new').click(function () {
        var addContent = "<div class='form-group'>";
        addContent += "<input type='text' class='form-control' id='filterSourceField" + filterCount + "' name='filterSourceField" + filterCount + "' placeholder='Source Field'>";
        addContent += "<select class='form-control' id='filterValueComp" + filterCount + "' name='filterValueComp" + filterCount + "'>";
        addContent += "<option value='eq'>==</option>";
        addContent += "<option value='gt'>></option>";
        addContent += "<option value='lt'><</option>";
        addContent +=  "</select>";
        addContent += "<input type='text' class='form-control' id='filterValue" + filterCount + "' name='filterValue" + filterCount + "' placeholder='Value'>";
        addContent += "<button type='button' class='glyphicon glyphicon-remove-circle' id='filterCancel" + filterCount + "'></button";
        addContent += "</div>";
        addContent += "<br>";

        $(addContent).appendTo('#filters');
        makeFilterCancelButtonFunction(filterCount);
        filterCount++;
    });

    function makeTransformCancelButtonFunction(count) {
        $('#transformCancel' + count).click(function () {
            $('#transformSourceField' + count).remove();
            $('#transformValueComp' + count).remove();
            $('#transformValue' + count).remove();
            $('#transformDestinationField' + count).remove();
            $('#transformCancel' + count).remove();
        });
    }

    function makeMappingCancelButtonFunction(count) {
        $('#mappingCancel' + count).click(function () {
            $('#mappingSourceField' + count).remove();
            $('#mappingDestinationField' + count).remove();
            $('#mappingCancel' + count).remove();
        });
    }

    function makeFilterCancelButtonFunction(count) {
            $('#filterCancel' + count).click(function () {
                $('#filterSourceField' + count).remove();
                $('#filterValueComp' + count).remove();
                $('#filterValue' + count).remove();
                $('#filterCancel' + count).remove();
            });
    }

});