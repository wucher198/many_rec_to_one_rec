import groovy.sql.GroovyRowResult
import groovy.sql.Sql

/** @author Maciej Smolka on 2016-04-06, PL  */

def connection = Sql.newInstance('jdbc:oracle:thin:@localhost:1521:G3HIS', 'g3his', 'g3his', 'oracle.jdbc.driver.OracleDriver')
def sql = "select * from TSM_ALIASINDEX"
ArrayList<GroovyRowResult> list = connection.rows(sql);
Map<String, HashMap<String, Object>> rowWithIndexes = new LinkedHashMap<>()

//[valid, activePartition, planned, provider.orgUnitMc]
//0 = {groovy.sql.GroovyRowResult@2580}  size = 8
// 0 = {java.util.LinkedHashMap$Entry@2682} "PRIMARYKEYID" -> "MIK6ED1UNLLD87PGBN"
// 1 = {java.util.LinkedHashMap$Entry@2683} "DOCUMENTID" -> "MIK6CE1ULPTH84ECA1"
// 2 = {java.util.LinkedHashMap$Entry@2684} "ALIASNAME" -> "valid"
// 3 = {java.util.LinkedHashMap$Entry@2685} "STRSHORTVALUE" -> "null"
// 4 = {java.util.LinkedHashMap$Entry@2686} "DATEVALUE" -> "null"
// 5 = {java.util.LinkedHashMap$Entry@2687} "DATEFROM" -> "2016-04-06 00:00:00.0"
// 6 = {java.util.LinkedHashMap$Entry@2688} "DATETO" -> "2016-04-07 00:00:00.0"
// 7 = {java.util.LinkedHashMap$Entry@2689} "COMPANYID" -> "~"

list.forEach() { row ->
    String documentId = row.get("DOCUMENTID");

    HashMap<String, Object> newRow = rowWithIndexes.get(documentId)

    if (newRow == null) {
        newRow = new HashMap<>()
        rowWithIndexes.put(documentId, newRow)
    }

    String aliasName = row.get("ALIASNAME")
    // For each alias row convert to fields with DOCUMENT_ID as PK - create object with DOCUMENT ID and HASHMAP of values
    switch (aliasName) {
        case "valid":
            newRow.put("VALID_DATEFROM", row.get("DATEFROM"))
            newRow.put("VALID_DATETO", row.get("DATETO"))
            newRow.put("ACTICVE_PARTITION", null)
            newRow.put("PLANNED_FROM", null)
            newRow.put("PLANNED_TO", null)
            newRow.put("PROVIDER_ORGUNITMC", null)
            break;
        case "activePartition":
            //  ACTICVE_PARTITION
            newRow.put("VALID_DATEFROM", null)
            newRow.put("VALID_DATETO", null)
            newRow.put("ACTICVE_PARTITION", row.get("STRSHORTVALUE"))
            newRow.put("PLANNED_FROM", null)
            newRow.put("PLANNED_TO", null)
            newRow.put("PROVIDER_ORGUNITMC", null)
            break;
        case "planned":
            // PLANNED_FROM
            // PLANNED_TO
            newRow.put("VALID_DATEFROM", null)
            newRow.put("VALID_DATETO", null)
            newRow.put("ACTICVE_PARTITION", null)
            newRow.put("PLANNED_FROM", row.get("DATEFROM"))
            newRow.put("PLANNED_TO", row.get("DATETO"))
            newRow.put("PROVIDER_ORGUNITMC", null)
            break;
        case "provider.orgUnitMc":
            // PROVIDER_ORGUNITMC
            newRow.put("VALID_DATEFROM", null)
            newRow.put("VALID_DATETO", null)
            newRow.put("ACTICVE_PARTITION", null)
            newRow.put("PLANNED_FROM", null)
            newRow.put("PLANNED_TO", null)
            newRow.put("PROVIDER_ORGUNITMC", row.get("STRSHORTVALUE"))
            break;
        default:
            break;
    }
}

rowWithIndexes.forEach() { id, newRow ->
    String columnsName = ""
    String values = ""

    newRow.forEach() { key, value ->
        columnsName += key + ","
        values += value + ","
    }

    columnsName.substring(0, columnsName.lastIndexOf(","))
    columnsName.substring(0, values.lastIndexOf(","))
    println("insert into TSM_INDEXS (" + columnsName + ") values (" + values + ")")
}

println()