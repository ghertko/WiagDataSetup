"""
2022-10-24
update WIAG data in a Jupyter-Notebook
item type: Bischof GS
preparation
   read canons from GS first
   log in to a database (`Wds.dbwiag`)
   set `data_path` ???
   set `gs_db`
"""

using Dates


"""
    insert_reference_volume(Wds, source_file)

insert references into reference_volume
"""
function insert_reference_volume!(table, source_table)
    @assert @isdefined(Wds) "'Wds' ist nicht definiert"

    @info "reading " source_table
    table_name = gs_db * "." * source_table
    sql = "SELECT id, titel AS title, autoren AS author, nummer AS number_vol, " *
        "uri AS online_resource, kurztitel AS shorttitle " *
        "FROM $(table_name)";
    df = Wds.sql_df(sql);

    Wds.clean_up!(df)

    @info "Zeilen, Spalten: " size(df)

    insertcols!(df, :item_type_id => item_type_id)

    columns = [
        :id => :reference_id,
        :item_type_id => :item_type_id,
        :title => :full_citation,
        :author => :author_editor,
        :online_resource => :online_resource,
        :shorttitle => :title_short,
        :number_vol => :gs_volume_nr,
    ]


    Wds.filltable!(table, select(df, columns));

end

"""
    gsn(dbwiag_name::String)

retrieve items that are referenced by item_type "Bischof"
"""
function gsn(dbwiag_name::String; item_type_bishop = 4)

    table_item_bishop = dbwiag_name * ".item"
    table_id_external_bishop = dbwiag_name * ".id_external"
    sql = "SELECT item.id as ep_item_id, e.value AS gsn " *
        "FROM $(table_item_bishop) " *
        "JOIN $(table_id_external_bishop) AS e ON item.id = e.item_id " *
        "WHERE item_type_id = $(item_type_bishop) " *
        "AND authority_id = 200 " *
        "AND item.is_online";
    @info sql
    return Wds.sql_df(sql);
end


function insert_item!(table,
                      gsn_table,
                      items_table,
                      persons_table,
                      df_gsn;
                      user_id = 40)

    gs_gsn_table = gs_db * ".gsn"
    gs_items_table = gs_db * ".items"
    gs_persons_table = gs_db * ".persons"
    sql = "SELECT g.id as gsn_id, item_id, g.nummer, items.status " *
        "FROM $(gs_gsn_table) AS g " *
        "JOIN $(gs_items_table) as i ON item_id = i.id " *
        "AND NOT i.deleted AND i.status = 'online'"
    df_item = Wds.sql_df(sql)

    # TODO read person.id for item.id_in_source


    df_item = innerjoin(df_item, df_gsn, on = :nummer => :gsn)

    select!(df_item, [

    insertcols!(
        df_item,
        1,
        :item_type_id => item_type_id
        :edit_status => "importiert",
        :created_by => user_id,
        :date_created => Dates.now(),
        :changed_by => user_id,
        :date_changed => Dates.now(),
        :id_public => missing,
        :is_online => 1
    )



end





"""
    insert_item!(source_file;
                 online_status = "fertig",
                 id_public_key = "Pers-EPISCGatz",
                 user_id = 40)

insert bishop meta data into item
"""
function insert_item_bishop!(table,
                      source_file;
                      online_status = "fertig",
                      id_public_key = "Pers-EPISCGatz",
                      user_id = 40)

    @assert @isdefined(Wds) "'Wds' ist nicht definiert"

    df_p = CSV.read(
        source_file,
        DataFrame,
        types = Dict(
            :ID_Bischof => String,
            :VIAF_ID => String,
            :GND_ID => String,
            :Wikidata_ID => String,
            :GSN_ID => String,
        ))
    Wds.clean_up!(df_p)

    @info "Zeilen, Spalten: " size(df_p)

    columns = [
        :ID_Bischof => :id_in_source,
        :fertig => :fertig,
        Symbol("Eintrag RI OPAC") => :item_in_source,
    ]
    df_item = select(df_p, columns);

    # is_online
    # bishop data have no column edit_status but "fertig"
    is_online(x) = ismissing(x) ? 0 : (x == online_status ? 1 : 0)
    transform!(df_item, :fertig => ByRow(is_online) => :is_online)

    # id_public
    make_id_episc(x) = Wds.make_id_public(x, 5, id_public_key)
    transform!(df_item, :id_in_source => ByRow(make_id_episc) => :id_public);

    # meta data
    date_time_now = Dates.format(now(), Dates.dateformat"yyyy-mm-dd HH:MM")

    insertcols!(
        df_item,
        :created_by => user_id,
        :date_created => date_time_now,
        :changed_by => user_id,
        :date_changed => date_time_now,
        :item_type_id => item_type_id,
    );

    Wds.filltable!("item", select(df_item, Not(:fertig)))

    return df_p

end

"""
    insert_person(df_p::AbstractDataFrame)

get IDs from `item` and `religious_order`; insert data into `person`
"""
function insert_person!(table, df_p::AbstractDataFrame)
    @assert @isdefined(Wds) "'Wds' ist nicht definiert"

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_ep = innerjoin(df_idx, df_p, on = :id_in_source => :ID_Bischof);

    # assign religious order
    sql = "SELECT id as religious_order_id, abbreviation FROM religious_order";
    df_rel_order = Wds.sql_df(sql);
    df_ep = leftjoin(df_ep, df_rel_order, on = :Orden => :abbreviation, matchmissing = :notequal);

    # - info
    unknown_order = String[];
    for row in eachrow(df_ep)
        if !ismissing(row[:Orden]) && ismissing(row[:religious_order_id])
            push!(unknown_order, row[:Orden])
        end
    end
    @info "unbekannte Ordensbezeichnungen" unique(unknown_order);

    # numeric dates
    parse_date_birth(s) = Wds.parsemaybe(s, :lower)
    parse_date_death(s) = Wds.parsemaybe(s, :upper)

    transform!(df_ep, :Geburtsdatum => ByRow(parse_date_birth) => :num_date_birth);
    transform!(df_ep, :Sterbedatum => ByRow(parse_date_death) => :num_date_death);

    # item_type_id
    insertcols!(df_ep, :item_type_id => item_type_id);

    columns = [
        :id => :id,
        :item_type_id => :item_type_id,
        :Praefix => :prefixname,
        :Vorname => :givenname,
        :Familienname => :familyname,
        :Sterbedatum => :date_death,
        :Geburtsdatum => :date_birth,
        :Bemerkung_Red => :comment,
        :Kommentar_Name => :note_name,
        :Kommentar_Person => :note_person,
        :religious_order_id => :religious_order_id,
    ]

    Wds.filltable!(table, select(df_ep, columns))

end


"""
    join_gsn(table, df_p::AbstractDataFrame, gso_db)

return joined DataFrame and DataFrame with new entries
"""
function join_gsn(df_p::AbstractDataFrame, gso_db)
    @assert @isdefined(Wds) "'Wds' ist nicht definiert"

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_ep = innerjoin(df_idx, df_p, on = :id_in_source => :ID_Bischof);

    # GSN
    # find the current GSN
    # table gsn_gsn is an auxiliary table, see notebook 'Domherr-GS'
    sql = "SELECT id_new AS gs_gsn_id, nummer, nummer_neu
    FROM $(gso_db).gsn_gsn";
    df_gsn_gsn = Wds.sql_df(sql);
    df_ep_gsn = innerjoin(df_ep, df_gsn_gsn, on = :GSN_ID => :nummer, matchmissing = :notequal)

    # - info about new gsn
    df_info = subset(df_ep_gsn, [:GSN_ID, :nummer_neu] => ByRow(!isequal));
    df_info_out = select(df_info, [:id_in_source, :Vorname, :Familienname, :GSN_ID, :nummer_neu])

    return df_ep_gsn, df_info_out
end


function insert_gsn!(table, df_ep_gsn::AbstractDataFrame)
    return insert_id_external_auth!(table, df_ep_gsn, 200, :nummer_neu)
end

"""
    insert_id_external(df_p::AbstractDataFrame)

insert into `id_external`
"""
function insert_id_external!(table, df_p::AbstractDataFrame)
    @assert @isdefined(Wds) "'Wds' ist nicht definiert"

    # id in table authority and column name in df_p
    authorities = [
        1 => :GND_ID,
        2 => :Wikidata_ID,
        4 => :VIAF_ID,
    ]

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_ep = innerjoin(df_idx, df_p, on = :id_in_source => :ID_Bischof);

    for auth in authorities
        @info "Normdatum " auth.second
        insert_id_external_auth!(table, df_ep, auth.first, auth.second)
    end

end


function insert_wikipedia_url!(table, df_p::AbstractDataFrame)
    @assert @isdefined(Wds) "'Wds' ist nicht definiert"

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_ep = innerjoin(df_idx, df_p, on = :id_in_source => :ID_Bischof);

    authority = (3 => :URL_Wikipedia)

    transform!(df_ep, authority.second => ByRow(Wds.fix_Wikipedia_URL) => authority.second);

    return insert_id_external_auth!(table, df_ep, authority.first, authority.second)
end

"""
    insert_id_exteral_auth!(table, df_p::AbstractDataFrame, auth_id, auth_col::Symbol)

insert values for id_external by authority
"""
function insert_id_external_auth!(table, df_ep::AbstractDataFrame, auth_id, auth_col::Symbol)

    columns = [
        :id => :item_id,
        auth_col => :value
    ]
    df_id_ext = select(df_ep, columns);
    df_id_ext = dropmissing(df_id_ext, :value)

    insertcols!(df_id_ext, :authority_id => auth_id)

    Wds.filltable!(table, df_id_ext)

end


"""
    insert_name_variant!(table, df_p::AbstractDataFrame, col::Symbol)

insert name variants into `familyname_variant` and `givenname_variant`
"""
function insert_name_variant!(table, df_p::AbstractDataFrame, col::Symbol)
    @assert @isdefined(Wds) "'Wds' ist nicht definiert"

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_ep = innerjoin(df_idx, df_p, on = :id_in_source => :ID_Bischof);

    df_nv = dropmissing(df_ep, col)

    df_exp = Wds.expand_column(df_nv, col, delim = ",");

    columns = [
        :id => :person_id,
        col => :name,
    ]

    return Wds.filltable!(table, select(df_exp, columns))

end

function insert_name_lookup!(table, df_p)
    @assert @isdefined(Wds) "'Wds' ist nicht definiert"

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_ep = innerjoin(df_idx, df_p, on = :id_in_source => :ID_Bischof);

    columns = [
        :id => :id,
        :Vorname => :givenname,
        :Praefix => :prefix_name,
        :Familienname => :familyname,
        :Vorname_Varianten => :givenname_variant,
        :Familienname_Variante => :familyname_variant,
    ]

    df_nl = Wds.create_name_lookup(select(df_ep, columns))

    return Wds.filltable!(table, df_nl)

end
