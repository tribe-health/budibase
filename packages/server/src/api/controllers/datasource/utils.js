const { FieldTypes, AutoFieldSubTypes } = require("../../../constants")
const { InternalTables } = require("../../../db/utils")
const { save: extTableSave } = require("../table/external")
const { save: extRowSave, patch: extRowUpdate } = require("../row/external")
const { fetch } = require("../row/internal")

exports.processInternalTableForConversion = (table, handled) => {
  const tableId = table._id
  delete table._id
  delete table._rev
  let relationships = []
  // remove any relationships to the user from the schema
  for (let [key, column] of Object.entries(table.schema)) {
    const isUserRelationship =
      column.type === FieldTypes.LINK &&
      column.tableId === InternalTables.USER_METADATA
    if (
      isUserRelationship ||
      column.type === FieldTypes.FORMULA ||
      column.type === FieldTypes.ATTACHMENT
    ) {
      delete table.schema[key]
      continue
    }
    if (column.type !== FieldTypes.LINK) {
      continue
    }
    if (handled.indexOf(tableId) === -1) {
      // have to operate on relationships after all tables moved
      relationships.push(column)
      handled.push(column.tableId)
      handled.push(tableId)
    }
    delete table.schema[key]
  }
  const autoIdCol = Object.values(table.schema).find(
    col => col.autocolumn && col.subtype === AutoFieldSubTypes.AUTO_ID
  )
  if (autoIdCol) {
    table.primary = [autoIdCol.name]
  } else {
    table.primary = ["id"]
    table.schema["id"] = {
      type: FieldTypes.NUMBER,
      autocolumn: true,
    }
  }
  return { table, relationships, handled }
}

exports.saveExternalTable = (table, datasourceId) => {
  return extTableSave({
    request: {
      body: {
        ...table,
        // TODO: can only create SQL tables right now
        sql: true,
        sourceId: datasourceId,
      },
    },
  })
}

exports.saveExternalRow = (tableId, row) => {
  const input = {
    request: {
      body: row,
    },
    params: { tableId },
  }
  if (row._id) {
    return extRowUpdate(input)
  } else {
    return extRowSave(input)
  }
}

exports.getInternalRowsForTable = async tableId => {
  return fetch({
    params: {
      tableId,
    },
  })
}
