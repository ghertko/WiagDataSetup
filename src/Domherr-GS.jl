"""
2022-10-24
update WIAG data in a Jupyter-Notebook
item type: Domherr GS
preparation
   log in to a database (`Wds.dbwiag`)
   set `datapath`
"""

using Dates

function check_globals()
    @info Wds
    @info "" item_type_id
    @info "" gso_db
end

"""
    insert_reference_volume(Wds, data_file)

insert references into reference_volume
"""
function insert_reference_volume!(table, gso_table)

    @info "reading " table

    gso_table_qualified = gso_db * "." * gso_table

    sql = "SELECT id, titel, autoren, nummer, uri, kurztitel " *
        "FROM $(gso_table_qualified)";
    df = Wds.sql_df(sql);

    Wds.clean_up!(df)

    @info "Zeilen, Spalten: " size(df)

    insertcols!(df, :item_type_id => item_type_id)

    columns = [
        :item_type_id => :item_type_id,
        :id => :reference_id,
        :titel => :full_citation,
        :autoren => :author_editor,
        :uri => :online_resource,
        :kurztitel => :title_short,
        :nummer => :gs_volume_nr,
    ]

    Wds.filltable!(table, select(df, columns));

end

"""
    update_gs_citation!(table, gs_citation_file)

add extra field to references
"""
function update_gs_citation!(table, gs_citation_file)
    df_cit = CSV.read(gs_citation_file, DataFrame, delim="\t")
    Wds.clean_up!(df_cit);

    sql = "UPDATE $(table) SET gs_citation = ? WHERE item_type_id = ? AND reference_id = ?"
    stmt = DBInterface.prepare(Wds.dbwiag, sql);

    row_n = 0
    for row in eachrow(df_cit)
        reference_id, gs_citation = row[[:reference_id, :gs_citation]]
        DBInterface.execute(stmt, (gs_citation, item_type_id, reference_id))
        row_n += 1
    end
    DBInterface.close!(stmt)

    return row_n;
end

"""
    person_referenced(db_ref)

return DataFrame with items referenced by table `id_external` in `db_ref`
"""
function person_referenced(db_ref)
    # find referenced canons where the refenecing canon is online
    table_name = "item"
    sql = "SELECT item.id as dh_item_id, e.value AS gsn " *
        "FROM $(db_ref).item " *
        "JOIN $(db_ref).id_external AS e ON item.id = e.item_id " *
        "WHERE item_type_id = 5 " *
        "AND authority_id = 200 " *
        "AND item.is_online";

    df_gsn = Wds.sql_df(sql)

    @info "check for ambiguous GSN"
    df_group = groupby(df_gsn, :gsn)
    df_count = combine(df_group, nrow)
    greater_one(x) = x > 1
    df_mult = subset(df_count, :nrow => ByRow(greater_one))
    if size(df_mult, 1) > 0
        @info "Einige GSN werden mehrfach verwendet" df_mult
    end

    # drop entries with the same gsn
    df_gsn = unique(df_gsn, :gsn);
    @info "Zahl der eindeutigen GSN" size(df_gsn, 1)

    # read persons.id via gsn.item_id and persons.item_id
    # we get valid and invalid GSN, but this is no problem because invalid
    # GSN are dropped in the subsequent join operation
    table_gsn = gso_db * ".gsn"
    table_person = gso_db * ".persons"
    sql = "SELECT gsn.nummer AS gsn, p.id AS person_id " *
        "FROM $(table_gsn) AS gsn " *
        "JOIN $(table_person) AS p ON p.item_id = gsn.item_id " *
        "AND NOT p.deleted "

    df_person = Wds.sql_df(sql)

    df_person = innerjoin(df_gsn, df_person, on = :gsn)

    @info "Zahl der zugeordneten Einträge in `persons`" size(df_person, 1)

    return df_person
end


"""
    person_by_domstift(db_ref)

find persons in relevant institutions
"""
function person_by_domstift(db_ref)
    # find institutions (domstifte)
    table_institution = db_ref * ".institution"
    table_item_property = db_ref * ".item_property"
    sql = "SELECT id_gsn FROM $(table_institution) AS inst " *
        "JOIN $(table_item_property) AS prp ON inst.id = prp.item_id " *
        "WHERE prp.name = 'domstift_short'"
    df_dft = Wds.sql_df(sql);
    ids_dft = join(df_dft.id_gsn, ", ")

    # find persons which are online and with offices in one of the Domstifte
    table_office = gso_db * ".offices"
    table_person = gso_db * ".persons"
    table_item = gso_db * ".items"
    sql = "SELECT p.id as person_id, p.item_id as item_id " *
        "FROM $(table_office) AS o " *
        "JOIN $(table_person) AS p ON o.person_id = p.id AND NOT p.deleted " *
        "JOIN $(table_item) AS i ON i.id =  p.item_id AND i.status = 'online' " *
        "WHERE o.klosterid in ($(ids_dft)) AND NOT o.deleted " *
        "GROUP BY p.id "
    df_p_dft = Wds.sql_df(sql)

    # find gsn via item_id; gsn_gsn.nummer_neu contains valid GSNs
    table_gsn = gso_db * ".gsn_gsn"
    sql = "select item_id, nummer_neu AS gsn " *
        "FROM $(table_gsn) " *
        "GROUP BY item_id"
    df_gsn = Wds.sql_df(sql);

    # 2022-11-06: there is one entry with missing GSN
    df_p_dft = innerjoin(df_p_dft, df_gsn, on = :item_id)

    @info "Personen mit einem Amt in einem Domstift" size(df_p_dft, 1)

    return df_p_dft
end

"""
    read_person()

read person data from table `persons`
"""
function read_person()
    sql_columns = [
        "p.id as person_id",
        "i.id AS item_id",
        "i.status AS status",
        "namenspraefix",
        "vorname",
        "familienname",
        "sterbedatum",
        "geburtsdatum",
        "`orden`",
        "familiennamenvarianten",
        "vornamenvarianten",
        "namenszusatz",
        "anmerkungen",
        "titel",
        "gndnummer",
        "viaf",
    ]
    table_person = gso_db * ".persons"
    table_item = gso_db * ".items"
    sql = "SELECT $(join(sql_columns, ", ")) " *
        "FROM $(table_person) AS p " *
        "JOIN $(table_item) AS i ON i.id = p.item_id " *
        "WHERE i.status = 'online' AND NOT i.deleted " *
        "ORDER BY p.id";
    df_p = Wds.sql_df(sql);
    Wds.clean_up!(df_p);

    # drop entries where neither :vorname nor :familienname is valid
    is_valid_name(x) = !ismissing(x) && !isempty(strip(x))
    or_valid_name(a, b) = is_valid_name(a) || is_valid_name(b)

    subset!(df_p, [:vorname, :familienname] => ByRow(or_valid_name))

    return df_p
end

"""
    set_id_wiag(df_p, db_ref)

assign `id_wiag` or generate `id_wiag` for new entries

To avoid conflicts with 'Domherrendatenbank' we do not use `id_in_source`
for the generation of `id_public`. Instead IDs have an offset of 80000.
"""
function set_id_wiag(df_p, db_ref)

    # read IDs from `db_ref`
    # map on :id = persons.id = :id_in_source;
    # find highest(last) ID used for the generation of :id_public
    # sort by :id
    # set :id_public for new entries (= where it is missing)

    # reuse ids in `db_ref`
    table_name = db_ref * ".item"
    sql = "SELECT id_public, id_in_source " *
        "FROM $(table_name) " *
        "WHERE item_type_id = $(item_type_id)"
    df_id = Wds.sql_df(sql);

    function extract_id_wiag(id_public)
        rgx = r"WIAG-Pers-CANON-([0-9]+)-0[0-9][0-9]"
        rgm = match(rgx, id_public)
        if isnothing(rgm)
            @warn "no match for" id_public
            return missing
        else
            return parse(Int, rgm[1])
        end
    end

    transform!(df_id, :id_public => ByRow(extract_id_wiag) => :id_wiag)

    transform!(df_p, :person_id => ByRow(string) => :id_in_source)

    # ### test ###
    # fake new entries
    # df_id = df_id[1:end-78, :];

    df_p = leftjoin(df_p, df_id, on = :id_in_source)

    next_id_wiag = maximum(df_id.id_wiag) + 1;

    sort!(df_p, :person_id);

    new_n = 0
    for row in eachrow(df_p)
        if ismissing(row[:id_public])
            row[:id_wiag] = next_id_wiag # for debugging
            next_id_wiag += 1
            new_n += 1
        end
    end

    @info "Zahl der erzeugten IDs" new_n

    return df_p
end

"""
    insert_item!(table,
                 df_p::AbstractDataFrame,
                 online_status = "online",
                 id_public_key = "Pers-CANON",
                 user_id = 40)

insert bishop meta data into item
"""
function insert_item!(table,
                      df_p::AbstractDataFrame;
                      online_status = "online",
                      id_public_key = "Pers-CANON",
                      user_id = 40)

    columns = [
        :id_in_source => :id_in_source,
        :status => :edit_status,
        :id_wiag => :id_wiag,
    ]
    df_item = select(df_p, columns);

    # is_online
    is_online(x) = ismissing(x) ? 0 : (x == online_status ? 1 : 0)
    transform!(df_item, :edit_status => ByRow(is_online) => :is_online)

    # id_public is set for Domherren GS in an extra step
    make_id_public(x) = Wds.make_id_public(x, 5, id_public_key)
    transform!(df_item, :id_wiag => ByRow(make_id_public) => :id_public);

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

    Wds.filltable!(table, select(df_item, Not(:id_wiag)))

end

"""
    insert_person(df_p::AbstractDataFrame)

get IDs from `item` and `religious_order`; insert data into `person`
"""
function insert_person!(table, df_p::AbstractDataFrame)

    # read ids
    sql = "SELECT id, id_in_source FROM item where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame

    df_cn = innerjoin(df_idx, select(df_p, Not(:person_id)), on = :id_in_source)

    # assign religious order
    sql = "SELECT id as religious_order_id, abbreviation FROM religious_order";
    df_rel_order = Wds.sql_df(sql);
    df_cn = leftjoin(df_cn, df_rel_order, on = :orden => :abbreviation, matchmissing = :notequal);

    # - info
    unknown_order = String[];
    for row in eachrow(df_cn)
        if !ismissing(row[:orden]) && ismissing(row[:religious_order_id])
            push!(unknown_order, row[:orden])
        end
    end
    @info "unbekannte Ordensbezeichnungen" unique(unknown_order);

    # numeric dates
    parse_date_birth(s) = Wds.parsemaybe(s, :lower)
    parse_date_death(s) = Wds.parsemaybe(s, :upper)

    transform!(df_cn, :geburtsdatum => ByRow(parse_date_birth) => :num_date_birth);
    transform!(df_cn, :sterbedatum => ByRow(parse_date_death) => :num_date_death);

    # item_type_id
    insertcols!(df_cn, :item_type_id => item_type_id);

    columns = [
        :id => :id,
        :item_type_id => :item_type_id,
        :namenspraefix => :prefixname,
        :vorname => :givenname,
        :familienname => :familyname,
        :sterbedatum => :date_death,
        :geburtsdatum => :date_birth,
        :anmerkungen => :comment,
        :namenszusatz => :note_name,
        :religious_order_id => :religious_order_id,
        :num_date_birth => :num_date_birth,
        :num_date_death => :num_date_death,
    ]

    # return df_cn

    Wds.filltable!(table, select(df_cn, columns))

end

"""
    insert_item_property!(table,
                          df_p::AbstractDataFrame;
                          property_column = Symbol("titel"),
                          property_name = "academic_title")

write values in column `property` column into `table`
"""
function insert_item_property!(table,
                               df_p::AbstractDataFrame;
                               property_column = Symbol("titel"),
                               property_name = "academic_title")

    # read ids
    sql = "SELECT id, id_in_source FROM item where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_cn = innerjoin(df_idx, select(df_p, Not(:person_id)), on = :id_in_source => :id_in_source);


    # get property type id
    sql = "SELECT id FROM item_property_type " *
        "WHERE name = '$(property_name)'"
    df_ipt = Wds.sql_df(sql);

    if size(df_ipt, 1) < 1
        @error "Eigenschaft nicht vorhanden" property_name
    end

    property_type_id = df_ipt[1, :id]

    columns = [
        :id => :item_id,
        property_column => :value,
    ]

    dropmissing!(df_cn, property_column)
    df_ip = select(df_cn, columns)

    insertcols!(
        df_ip,
        :name => property_name, # redundant because of property_type_id
        :property_type_id => property_type_id,
    )

    Wds.filltable!(table, df_ip)

end

"""
    insert_item_reference!(table, df_p_id)

insert references into `item_reference`, `df_p_id` should map `items.id` (`item_id`) to `id_in_source`
"""
function insert_item_reference!(table, df_p_id)

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    @info "Zeilen, Spalten" size(df_idx)

    table_source = gso_db * ".locations";
    sql = "SELECT item_id as item_id, seiten, book_id  FROM $(table_source) " *
        "WHERE item_status = 'online' AND NOT deleted " *
        "GROUP BY item_id, book_id, seiten"
    df_ref = Wds.sql_df(sql);

    columns = [
        :item_id,
        :id_in_source,
    ]

    df_ref = innerjoin(df_ref, select(df_p_id, columns), on = :item_id)
    df_ref = innerjoin(df_ref, df_idx, on = :id_in_source)

    @info "Zeilen, Spalten" size(df_ref)

    is_bio(seiten) = contains(seiten, "<b>");

    df_ref_bio = subset(df_ref, :seiten => ByRow(is_bio))

    @info "Zeilen, Spalten (mit Biogramm)" size(df_ref_bio)


    insertcols!(df_ref_bio, :item_type_id => item_type_id)

    columns = [
        :id => :item_id,
        :item_type_id => :item_type_id,
        :seiten => :page,
        :book_id => :reference_id,
    ]

    Wds.filltable!(table, select(df_ref_bio, columns))

end


"""
    insert_id_external(df_p::AbstractDataFrame)

insert into `id_external`
"""
function insert_id_external!(table, df_p::AbstractDataFrame)

    # id in table authority and column name in df_p
    # +-----+--------------------------------------+
    # | id  | url_name_formatter                   |
    # +-----+--------------------------------------+
    # |   1 | Gemeinsame Normdatei (GND) ID        |
    # |   2 | Wikidata                             |
    # |   3 | Wikipedia-Artikel                    |
    # |   4 | VIAF-ID                              |
    # |   5 | WIAG-ID                              |
    # | 200 | Personendatenbank der Germania Sacra |
    # +-----+--------------------------------------+

    authorities = [
        1 => :gndnummer
        4 => :viaf
        200 => :gsn
    ]

    # read ids
    table_item = "item"
    sql = "SELECT id, id_in_source " *
        "FROM $(table_item) where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    columns = [auth.second for auth in authorities]
    push!(columns, :id_in_source)

    df_cn = innerjoin(df_idx, select(df_p, columns), on = :id_in_source);

    for auth in authorities
        @info "Normdatum " auth.second
        insert_id_external_auth!(table, df_cn, auth.first, auth.second)
    end

end

"""

"""
function join_ep_gsn(df_p::AbstractDataFrame)::AbstractDataFrame
    authority_id = 200
    @info "" authority_id

    table_item = "item"
    table_id_external = "id_external"
    sql = "SELECT i.id as ep, i.id_public, e1.value AS ep_gsn " *
        "FROM $(table_item) AS i " *
        "JOIN $(table_id_external) AS e1 ON e1.item_id = i.id AND e1.authority_id = $(authority_id) " *
        "WHERE i.item_type_id = $(bishop_item_type) AND i.is_online";
    df_ep = Wds.sql_df(sql)

    df_p_gsn = leftjoin(df_p, df_ep, on = :WIAG_ID_Bischof => :id_public, matchmissing = :notequal)

    # use bishop's GSN where appropriate
    for row in eachrow(df_p_gsn)
        if ismissing(row[:GSN_ID]) && !ismissing(row[:ep_gsn])
            row[:GSN_ID] = row[:ep_gsn]
        end
    end

    dropmissing!(df_p_gsn, :GSN_ID)

    insertcols!(df_p_gsn, :authority_id => authority_id)

    return df_p_gsn
end


"""
    function insert_gsn!(table, df_p_gsn::AbstractDataFrame)

insert GSN into `id_external`
"""
function insert_gsn!(table, df_p_gsn::AbstractDataFrame)
    authority_id = 200
    @info "" authority_id

    return insert_id_external_auth!(table, df_p_gsn, authority_id, :nummer_neu)
end


"""
    insert_wikipedia_url!(table, df_p::AbstractDataFrame)

insert Wikipedia URL into `id_external`
"""
function insert_wikipedia_url!(table, df_p::AbstractDataFrame)
    @assert @isdefined(Wds) "'Wds' ist nicht definiert"

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    df_p = innerjoin(df_idx, df_p, on = :id_in_source => :ID_Domherr);

    authority = (3 => :URL_Wikipedia)

    transform!(df_p, authority.second => ByRow(Wds.fix_Wikipedia_URL) => authority.second);

    return insert_id_external_auth!(table, df_p, authority.first, authority.second)
end

"""
    insert_id_exteral_auth!(table, df_p::AbstractDataFrame, auth_id, auth_col::Symbol)

auxiliary function: insert values for id_external by authority
"""
function insert_id_external_auth!(table, df_p::AbstractDataFrame, auth_id, auth_col::Symbol)

    columns = [
        :id => :item_id,
        auth_col => :value
    ]
    df_id_ext = select(df_p, columns);
    df_id_ext = dropmissing(df_id_ext, :value)

    # some entries contain a complete URL, e.g. for WIAG_ID_Bischof
    function extract_id_value(data)
        data = strip(data)
        val = data
        if (length(data) > 3) && (data[1:4] == "http")
            p_list = split(data, "/")
            val = p_list[end]
        end
        return val
    end

    transform!(df_id_ext, :value => ByRow(extract_id_value) => :value)

    insertcols!(df_id_ext, :authority_id => auth_id)

    Wds.filltable!(table, df_id_ext)

end



"""
    insert_name_variant!(table, df_p::AbstractDataFrame, col::Symbol)

insert name variants into `familyname_variant` and `givenname_variant`
"""
function insert_name_variant!(table, df_p::AbstractDataFrame, col::Symbol)

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    columns = [
        col,
        :id_in_source,
    ]

    df_p = innerjoin(df_idx, select(df_p, columns), on = :id_in_source);

    df_nv = dropmissing(df_p, col)

    df_exp = Wds.expand_column(df_nv, col, delim = ",");

    columns = [
        :id => :person_id,
        col => :name,
    ]

    return Wds.filltable!(table, select(df_exp, columns))

end

"""
    insert_name_lookup!(table, df_p)

build name variant combinations and write them to `table`
"""
function insert_name_lookup!(table, df_p)
    @assert @isdefined(Wds) "'Wds' ist nicht definiert"

    # read ids
    sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
    df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

    columns = [
        :id_in_source => :id_in_source,
        :vorname => :givenname,
        :namenspraefix => :prefix_name,
        :familienname => :familyname,
        :familiennamenvarianten => :familyname_variant,
        :vornamenvarianten => :givenname_variant,
    ]

    df_cn = innerjoin(df_idx, select(df_p, columns), on = :id_in_source);

    df_nl = Wds.create_name_lookup(select(df_cn, Not(:id_in_source)))

    return Wds.filltable!(table, df_nl)

end

"""
    function map_cn_sources(table)

resolve reference from bishops to canons and canons in GS; fill `canon_lookup`
"""
function map_cn_sources(table)

    base_cols = [
        :person_id_name,
        :role_1,
        :role_2,
        :role_3
    ]

    df = DataFrame(Pair.(base_cols, repeat([Int[]], length(base_cols))))

    # find references dh -> ep
    # for entries
    # ep; dh gs ep
    # ep; dh ep
    df_dh_ep = find_dh_ep()
    columns = [
        :ep_id => :person_id_name,
        :dh_id => :role_1,
    ]

    df_in = select(df_dh_ep, columns)

    insertcols!(
        df_in,
        :role_2 => missing,
        :role_3 => missing,
    )

    df = vcat(df, df_in)

    @info "Domherren mit Veweisen auf Bischöfe" size(df);

    #- find references ep -> gs in the current set to set :role_2
    df_ep_gs = find_gs_via_ep()
    columns = [
        :ep_id => :person_id_name,
        :gs_id => :ep_role_gs
    ]
    df = leftjoin(df, select(df_ep_gs, columns), on = :person_id_name);

    #- find references dh -> gs als alternatives for :role_2
    # df_dh_gs = find_gs_via_dh_in_ep()
    df_dh_gs = find_gs_via_dh_in_ep()
    columns = [
        :dh_id => :role_1,
        :gs_id => :dh_role_gs,
    ]

    df = leftjoin(df, select(df_dh_gs, columns), on = :role_1)

    not_missing_not_equal(a, b) = ismissing(a) || ismissing(b) ? false : (a != b)

    df_mismatch = subset(df, [:ep_role_gs, :dh_role_gs] => ByRow(not_missing_not_equal))

    if size(df_mismatch, 1) > 0
        msg = "Verweise auf Domherren GS stimmen nicht überein (Verweise von Bischöfen haben Vorrang)"
        @warn msg df_mismatch
    end

    #- set role_2 and role_3
    for row in eachrow(df)
        row.role_2 = !ismissing(row.ep_role_gs) ? row.ep_role_gs : row.dh_role_gs
        if ismissing(row.role_2)
            row.role_2 = row.person_id_name
        else
            row.role_3 = row.person_id_name
        end
    end

    select!(df, base_cols)

    # add references based on canons
    # for entries
    # dh; dh gs
    # dh; dh

    df_dh_gs = find_dh_gs();
    #- drop entries already referenced by bishops
    df_dh_gs = antijoin(df_dh_gs, df, on = :dh_id => :role_1);

    @info "Verweise von Domherren auf Domherren-GS" size(df_dh_gs)

    columns = [
        :dh_id => :person_id_name,
        :dh_id => :role_1,
        :gs_id => :role_2,
    ]

    df_in = select(df_dh_gs, columns)
    insertcols!(df_in, :role_3 => missing)

    df = vcat(df, df_in)

    #- canons without references to other sources
    sql = "SELECT id AS dh_id FROM item WHERE item_type_id = 5 AND is_online";
    df_dh = Wds.sql_df(sql)

    df_dh = antijoin(df_dh, df, on = :dh_id => :role_1)
    @info "Restliche Domherren" size(df_dh)

    columns = [
        :dh_id => :person_id_name,
        :dh_id => :role_1,
    ]
    df_in = select(df_dh, columns);

    insertcols!(df_in, :role_2 => missing, :role_3 => missing)
    df = vcat(df, df_in)

    # bishops with references to canons GS
    # for entries
    # ep; gs ep

    df_ep_gs = find_ep_gs()

    #- find indirect references via GS
    # 2022-12-07 debug
    return df, df_ep_gs


    df_ep_gs = antijoin(df_ep_gs, df, on = :ep_id => :person_id_name)
    @info "Restlice Bischöfe mit Verweis auf Domherren GS" size(df_ep_gs)

    columns = [
        :ep_id => :person_id_name,
        :ep_id => :role_1,
        :gs_id => :role_2
    ]
    df_in = select(df_ep_gs, columns);

    insertcols!(df_in, :role_3 => missing)
    df = vcat(df, df_in)

    # canons GS not referenced so far
    # for entries
    # gs; gs
    sql = "SELECT item.id AS gs_id FROM item WHERE item_type_id = 6 AND is_online"
    df_gs = Wds.sql_df(sql)

    df_gs = antijoin(df_gs, df, on = :gs_id => :role_2, matchmissing = :notequal)
    @info "Restlich Domherren GS, auf die nicht verwiesen wird" size(df_gs)

    columns = [
        :gs_id => :person_id_name,
        :gs_id => :role_1
    ]
    df_in = select(df_gs, columns);

    insertcols!(df_in, :role_2 => missing, :role_3 => missing)
    df = vcat(df, df_in)


    # return Wds.filltable!(table, df_nl, clear_table = true)
    df_stack = DataFrame(person_id_name = Int[], person_id_role = Int[], prio_role = Int[])
    for row in eachrow(df)
        push!(df_stack, (row.person_id_name, row.role_1, 1))
        if !ismissing(row.role_2)
            push!(df_stack, (row.person_id_name, row.role_2, 2))
        end
        if !ismissing(row.role_3)
            push!(df_stack, (row.person_id_name, row.role_3, 3))
        end
    end

    return Wds.filltable!(table, df_stack, clear_table = true)

end

function map_sources(table)

    base_cols = [
        :person_id_name,
        :dh_id,
        :gs_id,
        :ep_id,
    ]

    df = DataFrame(Pair.(base_cols, repeat([Int[]], length(base_cols))))

    # find references dh -> ep
    # for entries
    # ep; dh gs ep
    # ep; dh ep
    df_dh_ep = find_dh_ep()
    columns = [
        :ep_id => :person_id_name,
        :dh_id => :dh_id,
        :ep_id => :ep_id,
    ]

    df_in = select(df_dh_ep, columns)

    insertcols!(
        df_in,
        :gs_id => missing,
    )

    df = vcat(df, df_in)

    @info "Domherren mit Veweisen auf Bischöfe" size(df);

    #- find references ep -> gs in the current set
    df_ep_gs = find_gs_via_ep()
    columns = [
        :ep_id => :person_id_name,
        :gs_id => :ep_gs_id
    ]
    df = leftjoin(df, select(df_ep_gs, columns), on = :person_id_name);

    #- find references dh -> gs als alternatives for :gs_id
    # df_dh_gs = find_gs_via_dh_in_ep()
    df_dh_gs = find_gs_via_dh_in_ep()
    columns = [
        :dh_id => :dh_id
        :gs_id => :dh_gs_id
    ]

    df = leftjoin(df, select(df_dh_gs, columns), on = :dh_id)

    not_missing_not_equal(a, b) = ismissing(a) || ismissing(b) ? false : (a != b)

    df_mismatch = subset(df, [:ep_gs_id, :dh_gs_id] => ByRow(not_missing_not_equal))

    if size(df_mismatch, 1) > 0
        msg = "Verweise auf Domherren GS stimmen nicht überein (Verweise von Bischöfen haben Vorrang)"
        @warn msg df_mismatch
    end

    #- set gs_id
    get_not_missing(a, b) = !ismissing(a) ? a : b

    transform!(df, [:ep_gs_id, :dh_gs_id] => ByRow(get_not_missing) => :gs_id)

    select!(df, base_cols)

    # find references dh -> gs and ep -> gs
    # for entries
    # dh; dh gs
    # ep; dh gs ep (indirect)
    # ep; gs ep

    df_dh_gs = find_dh_gs()
    df_ep_gs = find_ep_gs()

    #- drop entries already referenced by bishops
    df_dh_gs = antijoin(df_dh_gs, df, on = :dh_id);

    #- find remaining intersection dh -> gs, ep -> gs
    #- this set is empty when all bishops are referenced by canons
    df_gs_double = innerjoin(df_ep_gs, df_dh_gs, on = :gs_id)

    @info "Indirekte Verweise auf Bischöfe durch gemeinsame Verweise auf Domherren GS" df_gs_double

    columns = [
        :ep_id => :person_id_name,
        :dh_id => :dh_id,
        :gs_id => :gs_id,
        :ep_id => :ep_id,
    ]

    df = vcat(df, select(df_gs_double, columns))

    df_dh_gs = antijoin(df_dh_gs, df_gs_double, on = :dh_id)

    @info "Verweise von Domherren auf Domherren-GS" size(df_dh_gs)

    columns = [
        :dh_id => :person_id_name,
        :dh_id => :dh_id,
        :gs_id => :gs_id,
    ]

    df_in = select(df_dh_gs, columns)
    insertcols!(df_in, :ep_id => missing)

    df = vcat(df, df_in)

    df_ep_gs = antijoin(df_ep_gs, df, on = :ep_id, matchmissing = :notequal)
    @info "Restlice Bischöfe mit Verweis auf Domherren GS" size(df_ep_gs)

    columns = [
        :ep_id => :person_id_name,
        :ep_id => :ep_id,
        :gs_id => :gs_id,
    ]
    df_in = select(df_ep_gs, columns);

    insertcols!(df_in, :dh_id => missing)
    df = vcat(df, df_in)

    # canons without references to other sources
    # for entries
    # dh; dh
    sql = "SELECT id AS dh_id FROM item WHERE item_type_id = 5 AND is_online";
    df_dh = Wds.sql_df(sql)

    df_dh = antijoin(df_dh, df, on = :dh_id, matchmissing = :notequal)
    @info "Restliche Domherren" size(df_dh)

    columns = [
        :dh_id => :person_id_name,
        :dh_id => :dh_id,
    ]
    df_in = select(df_dh, columns);

    insertcols!(df_in, :gs_id => missing, :ep_id => missing)
    df = vcat(df, df_in)


    # canons GS not referenced so far
    # for entries
    # gs; gs
    sql = "SELECT item.id AS gs_id FROM item WHERE item_type_id = 6 AND is_online"
    df_gs = Wds.sql_df(sql)

    df_gs = antijoin(df_gs, df, on = :gs_id, matchmissing = :notequal)
    @info "Restlich Domherren GS, auf die nicht verwiesen wird" size(df_gs)

    columns = [
        :gs_id => :person_id_name,
        :gs_id => :gs_id,
    ]
    df_in = select(df_gs, columns);

    insertcols!(df_in, :dh_id => missing, :ep_id => missing)
    df = vcat(df, df_in)

    df_stack = DataFrame(person_id_name = Int[], person_id_role = Int[], prio_role = Int[])
    for row in eachrow(df)
        if !ismissing(row.dh_id)
            push!(df_stack, (row.person_id_name, row.dh_id, 1))
            if !ismissing(row.gs_id)
                push!(df_stack, (row.person_id_name, row.gs_id, 2))
                if !ismissing(row.ep_id)
                    push!(df_stack, (row.person_id_name, row.ep_id, 3))
                end
            elseif !ismissing(row.ep_id)
                push!(df_stack, (row.person_id_name, row.ep_id, 2))
            end
        else
            if !ismissing(row.gs_id)
                push!(df_stack, (row.person_id_name, row.gs_id, 1))
                if !ismissing(row.ep_id)
                    push!(df_stack, (row.person_id_name, row.ep_id, 2))
                end
            elseif !ismissing(row.ep_id)
                push!(df_stack, (row.person_id_name, row.ep_id, 1))
            end
        end
    end

    return Wds.filltable!(table, df_stack, clear_table = true)

end


"""
    find_dh_ep()

find bishops referenced by canons that are online
"""
function find_dh_ep()
    # find references dh -> ep
    table_id_external = "id_external"
    table_item = "item"
    sql = "SELECT ext.item_id AS dh_id, i.id AS ep_id " *
        "FROM $(table_id_external) AS ext " *
        "JOIN $(table_item) AS i ON i.id_public = ext.value AND ext.authority_id = 5 " *
        "JOIN $(table_item) AS i_dh ON i_dh.id = ext.item_id " *
        "WHERE i_dh.is_online";
    df_dh_ep = Wds.sql_df(sql);

    return df_dh_ep
end




"""
    find_ep_gs()

find bishops with a reference to a canon in GS
"""
function find_gs_via_ep()

    # find references ep -> gs
    table_id_external = "id_external"
    table_item = "item"
    sql = "SELECT ext_ep.item_id AS ep_id, ext_ep.value AS gsn " *
        "FROM $(table_id_external) AS ext_ep " *
        "JOIN $(table_item) AS item_ep ON item_ep.id = ext_ep.item_id AND item_ep.item_type_id = 4 " *
        "WHERE ext_ep.authority_id = 200"
    df_ep_gsn = Wds.sql_df(sql);

    sql = "SELECT ext_gs.item_id AS gs_id, ext_gs.value AS gsn " *
        "FROM $(table_id_external) AS ext_gs " *
        "JOIN $(table_item) AS item_gs ON item_gs.id = ext_gs.item_id AND item_gs.item_type_id = 6 " *
        "WHERE ext_gs.authority_id = 200"
    df_gs_gsn = Wds.sql_df(sql);


    df_ep_gs = innerjoin(df_ep_gsn, df_gs_gsn, on = :gsn);

    return df_ep_gs

end

"""
    find_gs_via_dh_in_ep()

find reference dh -> gs for canons with a reference to a bishop

There is no need to filter for bishops with a reference to canons from GS
"""
function find_gs_via_dh_in_ep()

    table_id_external = "id_external"
    table_item = "item"
    sql = "select dh_gs.dh_id, i.id as gs_id " *
        "from $(table_id_external) as ext " *
        "join (select i.id as dh_id, ext_gs.value as gsn " *
        "from $(table_item) as i " *
        "join $(table_id_external) as ext_gs on ext_gs.item_id = i.id and ext_gs.authority_id = 200 " *
        "join $(table_id_external) as ext_ep on ext_ep.item_id = i.id and ext_ep.authority_id = 5 " *
        "where i.item_type_id = 5 and ext_ep.value is not null) as dh_gs on dh_gs.gsn = ext.value " *
        "join item as i on ext.item_id = i.id " *
        "where ext.authority_id = 200 and i.item_type_id = 6 ";
    df_dh_in_ep_gs = Wds.sql_df(sql);
    return df_dh_in_ep_gs

end

"""
    find_dh_gs()

find reference dh -> gs
"""
function find_dh_gs()

    table_id_external = "id_external"
    table_item = "item"
    sql = "SELECT i_dh.id as dh_id, i_gs.id as gs_id " *
        "FROM id_external AS ext_dh " *
        "JOIN item AS i_dh ON i_dh.id = ext_dh.item_id " *
        " AND i_dh.item_type_id = 5 AND i_dh.is_online AND ext_dh.authority_id = 200 " *
        "JOIN id_external AS ext_gs ON ext_gs.value = ext_dh.value " *
        "JOIN item AS i_gs ON i_gs.id = ext_gs.item_id " *
        " AND i_gs.item_type_id = 6 AND i_gs.is_online AND ext_gs.authority_id = 200 ";

    df_dh_gs = Wds.sql_df(sql)
    return df_dh_gs

end

"""
    find_ep_gs()

find reference ep -> gs
"""
function find_ep_gs()

    table_id_external = "id_external"
    table_item = "item"
    sql = "SELECT i_ep.id as ep_id, i_gs.id as gs_id " *
        "FROM id_external AS ext_ep " *
        "JOIN item AS i_ep ON i_ep.id = ext_ep.item_id " *
        " AND i_ep.item_type_id = 4 AND i_ep.is_online AND ext_ep.authority_id = 200 " *
        "JOIN id_external AS ext_gs ON ext_gs.value = ext_ep.value " *
        "JOIN item AS i_gs ON i_gs.id = ext_gs.item_id " *
        " AND i_gs.item_type_id = 6 AND i_gs.is_online AND ext_gs.authority_id = 200 ";

    df_ep_gs = Wds.sql_df(sql)
    return df_ep_gs

end
