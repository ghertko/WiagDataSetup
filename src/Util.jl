
"""
    first(c::DBInterface.Cursor) = iterate(c)[1]

utility: get first row of a result set
"""
first(c::DBInterface.Cursor) = iterate(c)[1]

"""
    count_table!(db, table, join_field)

count table with field item_id
"""
function count_table(db::MySQL.Connection, table, join_field)
    global item_type_id
    if join_field in ("item_id", "person_id")
        sql = "SELECT COUNT(*) as n FROM $(table) " *
            "WHERE $(join_field) IN " *
            "(SELECT id FROM item WHERE item_type_id = $(item_type_id))"
    elseif join_field == "item_type_id"
        sql = "SELECT COUNT(*) as n FROM $(table) " *
            "WHERE item_type_id = $(item_type_id)"
    end
    @info sql
    c = DBInterface.execute(db, sql)
    return first(c)[:n]
end

"""
    clear_table!(db, table, join_field)

clear table with field item_id
"""
function clear_table!(db::MySQL.Connection, table, join_field)
    global item_type_id
    if join_field in ("item_id", "person_id", "id")
        sql = "DELETE FROM $(table) " *
            "WHERE $(join_field) IN " *
            "(SELECT id FROM item WHERE item_type_id = $(item_type_id))"
    elseif join_field == "item_type_id"
        sql = "DELETE FROM $(table) " *
            "WHERE item_type_id = $(item_type_id)"
    end
    c = DBInterface.execute(db, sql)
    n_row = c.rows_affected
    @info "Zeilen " n_row
    n_row
end

"""
    clear_item_property!(db, property_type_id)

clear item_property for `property_type_id`
"""
function clear_item_property!(db::MySQL.Connection, table, join_field, property_name)
    global item_type_id

    # get property type id
    sql = "SELECT id FROM item_property_type " *
        "WHERE name = '$(property_name)'"
    c = DBInterface.execute(Wds.dbwiag, sql)
    property_type_id = first(c)[:id]

    sql = "DELETE FROM $(table) " *
        "WHERE $(join_field) IN " *
        "(SELECT id FROM item WHERE item_type_id = $(item_type_id)) " *
        "AND property_type_id = $(property_type_id)"
    c = DBInterface.execute(db, sql)
    n_row = c.rows_affected
    @info "Zeilen " n_row
    n_row
end

"""
    clear_tables!(db, table_list)

`table_list` with `table` where `table.first` = table name, `table.second` = join field
"""
function clear_tables!(db, table_list)
    @info "Item Typ " item_type_id

    df_table_n = count_item_type(db, table_list)
    @info df_table_n

    println("Einträge in diesen Tabellen löschen? (j/J)")
    input = readline()
    n = 0
    if input in ("Ja", "ja", "j", "J")
        for table in table_list
            sql = "DELETE FROM $(table.first) " *
                "WHERE $(table.second) IN " *
                "(SELECT id FROM item WHERE item_type_id = $(item_type_id))"
            # println(sql)
            DBInterface.execute(db, sql)
            n += 1
        end
    end

    @info "Betroffene Tabellen " n

    return n
end
