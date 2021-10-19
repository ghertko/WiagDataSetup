module WiagDataSetup

using MySQL
using Infiltrator
using DataFrames
using CSV

dbwiag = nothing

function setDBWIAG(;pwd = missing, host = "127.0.0.1", user = "wiag", db = "wiag")
    global dbwiag
    if !isnothing(dbwiag)
        DBInterface.close!(dbwiag)
    end

    if ismissing(pwd)
        println("Passwort für User ", user)
        pwd = readline()
    end
    dbwiag = DBInterface.connect(MySQL.Connection, host, user, pwd, db = db)
end

"""
    updatenamevariant()

obsolete: see namelookup
"""
function updatenamevariant(fieldsrc::AbstractString, tablename::AbstractString)::Int
    dbwiag = DBInterface.connect(MySQL.Connection, "localhost", "wiag", "Wogen&Wellen", db="wiag");


    DBInterface.execute(dbwiag, "DELETE FROM " * tablename);

    # Do not use the same DB connection with an open cursor to insert data
    dfsrc = DBInterface.execute(dbwiag,
                                "SELECT wiagid, " * fieldsrc
                                * " FROM person") |> DataFrame;

    tblid = 1
    for row in eachrow(dfsrc)
        id, fns = row
        # println("id: ", id)
        # println(fns)

        if ismissing(fns) || fns == "" continue end
        for nv in split(fns, r",|;")
            insertstmt = ("INSERT INTO " * tablename * " VALUES ("
                          * string(tblid) * ","
                          * string(id) * ","
                          * "'" * strip(nv) * "')")
            # println(insertstmt)
            DBInterface.execute(dbwiag, insertstmt)
            tblid += 1;
        end
    end
    return tblid;
end

"""
    fillera(tblera::AbstractString, tblperson::AbstractString, tbloffice::AbstractString, colnameid::AbstractString, checkisonline = true)::Int

Compute earliest and latest date for each person, identified by `colnameid` and `colnameidinoffice`.

"""
function fillera(tblera::AbstractString,
                 tblperson::AbstractString,
                 tbloffice::AbstractString,
                 colid = "id",
                 colidinoffice = "id_person";
                 checkisonline = false)::Int
    global dbwiag
    if isnothing(dbwiag)
        error("There is no valid database connection. Use `setDBWIAG'.")
    end

    msg = 1000

    DBInterface.execute(dbwiag, "DELETE FROM " * tblera);

    sqlselect = "SELECT " * colid * " as idperson, date_birth, date_death " *
        " FROM " * tblperson

    if checkisonline
        sqlselect *= " WHERE status = 'online'"
    end


    dfperson = DBInterface.execute(dbwiag, sqlselect) |> DataFrame;

    tblid = 0;

    # get office data
    sqlselect = "SELECT " * colidinoffice * ", date_start, date_end " * " FROM " * tbloffice
    dfoffice = DBInterface.execute(dbwiag, sqlselect) |> DataFrame

    insstmt = DBInterface.prepare(dbwiag, "INSERT INTO " * tblera * " VALUES (?, ?, ?)")

    for row in eachrow(dfperson)
        idperson, datebirth, datedeath = row[[:idperson, :date_birth, :date_death]]

        dfioffice = filter([Symbol(colidinoffice)] => isequal(idperson), dfoffice)

        erastart, eraend = extremaera(datebirth, datedeath, dfioffice)


        # it is a bit slower to call the database in each step, but needs less code
        DBInterface.execute(insstmt, [idperson, erastart, eraend]);
        tblid += 1
        if tblid % msg == 0
            @info tblid
        end
    end

    DBInterface.close!(insstmt)

    return tblid
end


"""
    updatecnera(tblera::AbstractString, tblperson::AbstractString, tblonline::AbstractString, tbloffice::AbstractString, colnameid::AbstractString)::Int

Compute earliest and latest date for each person, identified by `colnameid` and `colnameidinoffice`.

"""
function updatecnera(tblera::AbstractString,
                     tblonline::AbstractString,
                     tblperson::AbstractString,
                     tbloffice::AbstractString,
                     colid::AbstractString,
                     colidinoffice = "id_person")::Int
    # TODO take into account offices in bishop's database
    global dbwiag
    if isnothing(dbwiag)
        error("There is no valid database connection. Use `setDBWIAG'.")
    end

    msg = 1000

    @info "update '" * tblera * "'"

    sqlperson = "SELECT po.id as idonline, po." * colid * " as idperson " * ", date_birth, date_death " *
        " FROM " * tblonline * " AS po " *
        " JOIN " * tblperson * " AS dh ON dh.id = po." * colid

    dfperson = DBInterface.execute(dbwiag, sqlperson) |> DataFrame;

    tblid = 0;

    # get office data
    sqloffice = "SELECT " * colidinoffice * ", date_start, date_end " * " FROM " * tbloffice
    dfoffice = DBInterface.execute(dbwiag, sqloffice) |> DataFrame
    # @infiltrate

    sqlstart = "UPDATE " * tblera * " SET era_start=?" *
        " WHERE id_online=? AND (era_start is NULL OR era_start > ?)"
    updstmtstart = DBInterface.prepare(dbwiag, sqlstart)

    sqlend = "UPDATE " * tblera * " SET era_end=?" *
        " WHERE id_online=? AND (era_end is NULL OR era_end < ?)"
    updstmtend = DBInterface.prepare(dbwiag, sqlend)


    for row in eachrow(dfperson)

        idonline, idperson, datebirth, datedeath = row[[:idonline, :idperson, :date_birth, :date_death]]

        dfioffice = filter([Symbol(colidinoffice)] => isequal(idperson), dfoffice)

        erastart, eraend = eraextrema(datebirth, datedeath, dfioffice)


        # it is a bit slower to call the database in each step, but needs less code
        if !ismissing(erastart)
            DBInterface.execute(updstmtstart, [erastart, idonline, erastart])
        end
        if !ismissing(eraend)
            DBInterface.execute(updstmtend, [eraend, idonline, eraend])
        end

        tblid += 1
        if tblid % msg == 0
            @info tblid
        end
    end

    DBInterface.close!(updstmtstart)
    DBInterface.close!(updstmtend)

    return tblid
end


"""
    eraextrema(datebirth, datedeath, dfoffice)

find extrema in [`datebirth`, `datedeath`] and `dfoffice`
"""
function eraextrema(datebirth, datedeath, dfoffice)

    erastart = Inf
    eraend = -Inf


    vcand = parsemaybe(datebirth, :lower)
    if !ismissing(vcand) erastart = vcand end

    vcand = parsemaybe(datedeath, :upper)
    if ismissing(vcand)
        # we may have a date like "13. Jhd" only in datebirth
        vcand = parsemaybe(datebirth, :upper)
    end

    if !ismissing(vcand)
        eraend = vcand
    end

    for oc in eachrow(dfoffice)
        datestart = oc[:date_start]
        dateend = oc[:date_end]

        vcand = parsemaybe(datestart, :lower)
        if !ismissing(vcand) && vcand < erastart
            erastart = vcand
        end

        vcand = parsemaybe(dateend, :upper)
        if ismissing(vcand)
            # we may have a date like "13. Jhd"
            vcand = parsemaybe(datestart, :upper)
        end

        if !ismissing(vcand) && vcand > eraend
            eraend = vcand
        end

    end

    if erastart == Inf && eraend != -Inf
        erastart = eraend
    elseif erastart != Inf && eraend == -Inf
        eraend = erastart
    end

    if erastart == Inf
        erastart = missing
    end
    if eraend == -Inf
        eraend = missing
    end

    return erastart, eraend
end


"""
    fillpersondate(tblperson::AbstractString)::Int

parse `date_birth` and `date_death`.

"""
function fillpersondate(tblperson::AbstractString)::Int

    global dbwiag
    if isnothing(dbwiag)
        setDBWIAG()
    end

    msg = 1000

    sqlupdstart = "UPDATE " * tblperson *
        " SET numdate_birth = ?, date_hist_first = ?" *
        " WHERE id = ?";

    updstartstmt = DBInterface.prepare(dbwiag, sqlupdstart)

    sqlupdend = "UPDATE " * tblperson *
        " SET numdate_death = ?, date_hist_last = ?" *
        " WHERE id = ?";

    updendstmt = DBInterface.prepare(dbwiag, sqlupdend)

    sqldf = "SELECT id, date_birth, date_death FROM " * tblperson

    dfperson = DBInterface.execute(dbwiag, sqldf) |> DataFrame;

    tblid = 0
    for row in eachrow(dfperson)
        id, date_birth, date_death = row

        numdate_start = parsemaybe(date_birth, :lower)
        numdate_end = parsemaybe(date_death, :upper)
        if ismissing(numdate_end)
            numdate_end = parsemaybe(date_birth, :upper)
        end

        if !ismissing(numdate_start)
            DBInterface.execute(updstartstmt, [numdate_start, numdate_start, id])
        end

        if !ismissing(numdate_end)
            DBInterface.execute(updendstmt, [numdate_end, numdate_end, id])
        end

        tblid += 1
        if tblid % msg == 0
            @info tblid
        end
    end

    DBInterface.close!(updstartstmt)
    DBInterface.close!(updendstmt)

    return tblid
end




"""
    fillofficedateowntable(tblofficedate::AbstractString,
                       tbloffice::AbstractString,
                       colid::AbstractString,
                       colidperson = "wiagid",
                       colidinoffice = "wiagid_person",
                       tblperson = nothing)::Int

Extract dates as integer values. (2021-05-06 obsolete)
"""
function fillofficedateowntable(tblofficedate::AbstractString,
                                tbloffice::AbstractString;
                                colid::AbstractString = "id",
                                colidperson = "wiagid",
                                colidinoffice = "wiagid_person",
                                tblperson = nothing)::Int

    global dbwiag
    if isnothing(dbwiag)
        setDBWIAG()
    end

    msg = 1000

    sql = "SELECT o." * colid * ", date_start, date_end, id_monastery FROM " * tbloffice * " as o"

    if !isnothing(tblperson)
        sql *= " JOIN " * tblperson * " as p ON p." * colidperson * " = " * "o." * colidinoffice
    end

    dfoffice = DBInterface.execute(dbwiag, sql) |> DataFrame;

    tblid = 0;
    # do not clear the table
    @info "Append to table " tblofficedate
    sqli = "INSERT INTO " * tblofficedate * "(id_office, date_start, date_end) VALUES (?, ?, ?)"

    insstmt = DBInterface.prepare(dbwiag, sqli)

    for row in eachrow(dfoffice)
        id, date_start, date_end, id_monastery = row

        numdate_start = parsemaybe(date_start, :lower)
        numdate_end = parsemaybe(date_end, :upper)
        if ismissing(numdate_end)
            numdate_end = parsemaybe(date_start, :upper)
        end

        # push!(csqlvalues, "(" * id * ", " * numdate_start * ", " * numdate_end * ")")
        da = [id, numdate_start, numdate_end]
        DBInterface.execute(insstmt, da)
        tblid += 1
        if tblid % msg == 0
            @info tblid
        end
        # if tblid > 25 break end
    end

    DBInterface.close!(insstmt)
    #sqlvalues = join(csqlvalues, ", ")
    #DBInterface.execute(dbwiag, "INSERT INTO " * tblofficedate * " VALUES " * sqlvalues)

    return tblid
end

function fillofficedate(tbloffice::AbstractString;
                        colid::AbstractString = "id",
                        colidperson = "id_dh",
                        colidinoffice = "id_canon",
                        tblperson = nothing)::Int

    global dbwiag
    if isnothing(dbwiag)
        setDBWIAG()
    end

    msg = 1000

    sql = "SELECT o." * colid * ", date_start, date_end, id_monastery FROM " * tbloffice * " as o"

    # e.g. take only care of persons that are 'online'
    if !isnothing(tblperson)
        sql *= " JOIN " * tblperson * " as p ON p." * colidperson * " = " * "o." * colidinoffice
    end

    dfoffice = DBInterface.execute(dbwiag, sql) |> DataFrame;

    tblid = 0;
    # do not clear the table
    @info "Update table " tbloffice
    sqlstart = "UPDATE " * tbloffice * " SET numdate_start = ? WHERE id = ?"
    updstartstmt = DBInterface.prepare(dbwiag, sqlstart)

    sqlend = "UPDATE " * tbloffice * " SET numdate_end = ? WHERE id = ?"
    updendstmt = DBInterface.prepare(dbwiag, sqlend)

    for row in eachrow(dfoffice)
        id, date_start, date_end, id_monastery = row

        numdate_start = parsemaybe(date_start, :lower)
        numdate_end = parsemaybe(date_end, :upper)
        if ismissing(numdate_end)
            numdate_end = parsemaybe(date_start, :upper)
        end

        # push!(csqlvalues, "(" * id * ", " * numdate_start * ", " * numdate_end * ")")
        if !ismissing(numdate_start)
            DBInterface.execute(updstartstmt, [numdate_start, id])
        end

        if !ismissing(numdate_end)
            DBInterface.execute(updendstmt, [numdate_end, id])
        end

        tblid += 1
        if tblid % msg == 0
            @info tblid
        end
        # if tblid > 25 break end
    end

    DBInterface.close!(updstartstmt)
    DBInterface.close!(updendstmt)
    #sqlvalues = join(csqlvalues, ", ")
    #DBInterface.execute(dbwiag, "INSERT INTO " * tblofficedate * " VALUES " * sqlvalues)

    return tblid
end


"""
    fillofficelocation(tbloffice::AbstractString, tblmonasterylocation::AbstractString, tblplace::AbstractString; tblonline::AbstractString = nothing, colid::AbstractString)::Int

Find locations for offices that are related to a monastery or write the value of field diocese to location
"""
function fillofficelocation(tbloffice::AbstractString,
                            tblmonasterylocation::AbstractString,
                            tblplace::AbstractString,
                            tblonline::AbstractString = nothing,
                            colid::AbstractString = nothing)

    colnameofficeid = "id_office";

    global dbwiag
    if isnothing(dbwiag)
        setDBWIAG()
    end

    msg = 1000

    if isnothing(tblonline)
        sqlo = "SELECT o.id, id_monastery, location, diocese," *
            " o.numdate_start, o.numdate_end" *
            " FROM " * tbloffice * " as o"
    else
        sqlo = "SELECT o.id, id_monastery, location, diocese," *
            " o.numdate_start, o.numdate_end" *
            " FROM " * tbloffice * " as o" *
            " JOIN " * tblonline * " as l ON o.id_canon = l." * colid;
    end

    dfoffice = DBInterface.execute(dbwiag, sqlo) |> DataFrame;

    sqlml = "SELECT place_id, location_name, location_begin_tpq as loc_start, location_end_tpq as loc_end" *
        " FROM " * tblmonasterylocation * " WHERE location_name IS NOT NULL AND wiagid_monastery = ?"
    mlstmt = DBInterface.prepare(dbwiag, sqlml)

    sqlmlnn = "SELECT place_id, location_name, location_begin_tpq as loc_start, location_end_tpq as loc_end" *
        " FROM " * tblmonasterylocation * " WHERE location_name IS NULL AND wiagid_monastery = ?"
    mlnonamestmt = DBInterface.prepare(dbwiag, sqlmlnn)

    sqlp = "SELECT place_name FROM " * tblplace * " WHERE id_places IN (?)"
    plstmt = DBInterface.prepare(dbwiag, sqlp)

    sqlupd = "UPDATE " * tbloffice * " SET location_show = ? WHERE id = ?"
    updstmt = DBInterface.prepare(dbwiag, sqlupd)

    ntest = 100000
    ir = 0
    for row in eachrow(dfoffice)
        places = String[];
        id, id_monastery, location, diocese, date_start, date_end =
            row[[:id, :id_monastery, :location, :diocese, :numdate_start, :numdate_end]]

        # use values of `location` or `diocese` if present
        if !ismissing(location) && location != ""
            push!(places, location)
        elseif ismissing(id_monastery) || id_monastery == ""
            if !ismissing(diocese)
                push!(places, diocese)
            end
        else
            ffilter(loc_start, loc_end) = filterlocbydate(loc_start, loc_end, date_start, date_end)

            # monasteries where a location name is given
            ml = DBInterface.execute(mlstmt, [id_monastery]) |> DataFrame;
            nloc = size(ml, 1)
            if nloc == 1
                push!(places, ml[1, :location_name])
            elseif nloc > 1
                mlfilter = filter([:loc_start, :loc_end] => ffilter, ml)
                places = mlfilter[:, :location_name]
            else
                # monasteries where no location name is given
                mlnn = DBInterface.execute(mlnonamestmt, [id_monastery]) |> DataFrame;
                # filter only if there is more than one alternative
                if size(mlnn, 1) > 1
                    mlfilter = filter([:loc_start, :loc_end] => ffilter, mlnn)
                ids_place = mlfilter[:, :place_id]
                else
                    ids_place = mlnn[:, :place_id]
                end
                if length(ids_place) > 0
                    dfp = DBInterface.execute(plstmt, ids_place) |> DataFrame;
                    places = dfp[:, :place_name]
                else
                    @warn "No place found for office", id
                end
            end
        end
        if length(places) > 0
            locationshow = String(strip(places[1]))
            DBInterface.execute(updstmt, [locationshow, id])
            ir += 1
            if ir % msg == 0
                @info ir
            end
            if ir > ntest
                break
            end
        end

    end

    DBInterface.close!(updstmt)
    DBInterface.close!(plstmt)
    DBInterface.close!(mlnonamestmt)
    DBInterface.close!(mlstmt)

    return ir;
end

"""
    filterlocbydate(loc_start, loc_end, date_start, date_end)

Check if dates for the location and the office are compatibel with each other
"""
function filterlocbydate(loc_start, loc_end, date_start, date_end)
    loc_startint = parsemaybe(loc_start, :lower)
    if !ismissing(loc_startint) && !ismissing(date_end) && loc_startint > date_end
        return false
    end
    loc_endint = parsemaybe(loc_end, :upper)
    if !ismissing(loc_endint) && !ismissing(date_start) && loc_endint < date_start
        return false
    end
    return true
end

"""
    fillnamelookup(tbllookup::AbstractString,
                   tblperson::AbstractString,
                   colnameid::AbstractString = "id";
                   checkisonline = false)::Int

Fill `tablename` with combinations of givenname and familyname and their variants.
"""
function fillnamelookup(tbllookup::AbstractString,
                        tblperson::AbstractString,
                        colnameid::AbstractString = "id";
                        checkisonline = false)::Int
    msg = 200
    if isnothing(dbwiag)
        error("There is no valid database connection. Use `setDBWIAG'.")
    end

    DBInterface.execute(dbwiag, "DELETE FROM " * tbllookup)

    sql = "SELECT " * colnameid * " as id_person, " *
        "givenname, prefix_name, familyname, givenname_variant, familyname_variant " *
        "FROM " * tblperson * " person " *
        (checkisonline ? "WHERE status = 'online' " : "")

    dfperson = DBInterface.execute(dbwiag, sql) |> DataFrame

    # SQL
    # INSERT INTO dsttable VALUES (NULL, 'id_person1', 'givenname1', 'prefix_name1', 'familyname1'),
    # ('NULL', 'id_person2', 'givenname2', 'prefix_name2', 'familyname2');
    #
    # structure
    # gn[:] prefix fn|fnv
    # gn[1] prefix fn|fnv
    # gnv[:] prefix fn|fnv
    # gnv[1] prefix fn|fnv

    # In the web application choose a version with or without prefix.

    imsg = 0
    csqlvalues = String[]
    appendtosqlrow(row) = append!(csqlvalues, row)
    for row in eachrow(dfperson)
        idperson = row[:id_person]
        gn = row[:givenname]
        prefix = row[:prefix_name]
        fn = row[:familyname]
        gnv = row[:givenname_variant]
        fnv = row[:familyname_variant]

        fillnamelookupgn(idperson, gn, prefix, fn, fnv) |> appendtosqlrow

        if !ismissing(gnv) && gnv != ""
            # sets of givennames
            cgnv = split(gnv, r", *")
            for gnve in cgnv
                fillnamelookupgn(idperson, gnve, prefix, fn, fnv) |> appendtosqlrow
            end
        end
        imsg += 1

        if imsg % msg == 0
            @info imsg
        end
    end
    sqlvalues = join(csqlvalues, ", ")

    irowout = length(csqlvalues)

    DBInterface.execute(dbwiag, "INSERT INTO " * tbllookup * " VALUES " * sqlvalues)

    return irowout

end

"""
    fillcnnamelookup(tbllookup::AbstractString,
                     tblonline::AbstractString,
                     tblcanon::AbstractString,
                     tblcanongs::AbstractString)::Int

Fill `tablename` (for canons) with combinations of givenname and familyname and their variants.
"""
function fillcnnamelookup(tbllookup::AbstractString,
                          tblonline::AbstractString,
                          tblcanon::AbstractString,
                          tblcanongs::AbstractString)::Int
    msg = 400
    if isnothing(dbwiag)
        error("There is no valid database connection. Use `setDBWIAG'.")
    end

    DBInterface.execute(dbwiag, "DELETE FROM " * tbllookup)
    @info "clear " tbllookup

    sql = "SELECT id, id_dh, id_gs FROM " * tblonline

    dfonline = DBInterface.execute(dbwiag, sql) |> DataFrame

    sqldh = "SELECT id as id_person, " *
        "givenname, prefix_name, familyname, givenname_variant, familyname_variant " *
        "FROM " * tblcanon *
        " WHERE ID = ?"

    stmtdh = DBInterface.prepare(dbwiag, sqldh)
    sqlgs = "SELECT id as id_person, " *
        "givenname, prefix_name, familyname, givenname_variant, familyname_variant " *
        "FROM " * tblcanongs *
        " WHERE ID = ?"

    stmtgs = DBInterface.prepare(dbwiag, sqlgs)

    sqllookup = "INSERT INTO " * tbllookup * " VALUES (NULL, ?, ?, ?, ?, ?, ?)"

    stmtlookup = DBInterface.prepare(dbwiag, sqllookup)


    # SQL
    # INSERT INTO dsttable VALUES (NULL, 'id_person1', 'givenname1', 'prefix_name1', 'familyname1'),
    # ('NULL', 'id_person2', 'givenname2', 'prefix_name2', 'familyname2');
    #
    # structure
    # gn[:] prefix fn|fnv
    # gn[1] prefix fn|fnv
    # gnv[:] prefix fn|fnv
    # gnv[1] prefix fn|fnv

    # In the web application choose a version with or without prefix.

    imsg = 0

    for online in eachrow(dfonline)
        id_online, id_person = online[[:id, :id_dh]]
        nvars = 0
        if !ismissing(id_person)
            dfperson = DBInterface.execute(stmtdh, [id_person]) |> DataFrame
            if size(dfperson, 1) != 1
                @warn "No exact match for " id_online
            else
                nvars = insertlookuprows(stmtlookup, id_online, dfperson[1, :])
            end
        end
        id_online, id_person = online[[:id, :id_gs]]
        if !ismissing(id_person)
            dfperson = DBInterface.execute(stmtgs, [id_person]) |> DataFrame
            if size(dfperson, 1) != 1
                @warn "No exact match for " id_online
            else
                nvars += insertlookuprows(stmtlookup, id_online, dfperson[1, :])
            end
        end
        # @infiltrate id_online == "WIAG-Pers-CANON-80886-001"

        imsg += 1
        if imsg % msg == 0
            @info imsg
        end
    end

    return imsg
end


function insertlookuprows(stmt, id_online, row)
    gn = row[:givenname]
    prefix = row[:prefix_name]
    fn = row[:familyname]
    gnv = row[:givenname_variant]
    fnv = row[:familyname_variant]

    csqlvalues = String[]

    function insertvars(variants)
        for variant in variants
            DBInterface.execute(stmt, vcat([id_online], variant))
        end
    end

    nvars = 0
    vars = makevariantsgn(gn, prefix, fn, fnv)
    nvars += length(vars)
    insertvars(vars)

    if !ismissing(gnv) && gnv != ""
        # set of givennames
        cgnv = split(gnv, r", *")
        for gnve in cgnv
            vars = makevariantsgn(gnve, prefix, fn, fnv)
            nvars += length(vars)
            insertvars(vars)
        end
    end
    # @infiltrate id_online == "WIAG-Pers-CANON-80886-001"

    return nvars

end


"""
    sqlstring(s::AbstractString)::AbstractString

remove labels in data fields ("Taufname: Karl") and escape apostrophes
"""
function sqlstring(s::AbstractString)::AbstractString
    poslabel = findfirst(':', s)
    if !isnothing(poslabel)
        s = s[poslabel + 1:end]
    end
    s = replace(strip(s), "'" => "''")
    return s
end

"""
    makevariantsgn(gn, prefix, fn, fnv)

return an array of variants
"""
function makevariantsgn(gn, prefix, fn, fnv)
    csql = Vector{Vector{Union{String, Missing}}}()

    function getvalue(s)
        if ismissing(s) || s == "NULL" || s == ""
            return missing
        else
            return sqlstring(s)
        end
    end

    function pushcsql(gni, fni)
        sgni = getvalue(gni)
        sfni = getvalue(fni)
        sprefix = getvalue(prefix)
        # skip the prefix if it is contained in the variant
        if !ismissing(sprefix) && !ismissing(sfni) &&
            (occursin(sprefix * " ", sfni) || occursin(" " * sprefix * " ", sfni))
            sprefix = missing
        end
        values = [
            sgni,
            sprefix,
            sfni,
            ismissing(sfni) ? missing : sgni * " " * sfni,
            ismissing(sfni) || ismissing(sprefix) ? missing : sgni * " " * sprefix * " " * sfni
        ]
        push!(csql, values)
    end

    # pushcsql(gn, fn)
    cgn = split(gn);
    for gnsingle in cgn
        pushcsql(gnsingle, fn)
    end

    # more than one givenname; write complete name
    if length(cgn) > 1
        pushcsql(gn, fn)
    end

    # familyname variants
    if !ismissing(fnv) && strip(fnv) != ""
        cfnv = split(fnv, r", *")
        for fnve in cfnv
            pushcsql(gn, fnve)
            # more than one givenname
            for gnsingle in cgn
                pushcsql(gnsingle, fnve)
            end
        end
    end

    return csql
end

# parse time data
const rgpcentury = "([1-9][0-9]?)\\. (Jahrh|Jh)"
const rgpyear = "([1-9][0-9][0-9]+)"
const rgpyearfc = "([1-9][0-9]+)"

# turn of the century
const rgxtcentury = Regex("([1-9][0-9]?)\\./" * rgpcentury, "i")

# quarter
const rgx1qcentury = Regex("(1\\.|erstes) Viertel +(des )?" * rgpcentury, "i")
const rgx2qcentury = Regex("(2\\.|zweites) Viertel +(des )?" * rgpcentury, "i")
const rgx3qcentury = Regex("(3\\.|drittes) Viertel +(des )?" * rgpcentury, "i")
const rgx4qcentury = Regex("(4\\.|viertes) Viertel +(des )?" * rgpcentury, "i")

# begin, middle end
const rgx1tcentury = Regex("Anfang (des )?" * rgpcentury, "i")
const rgx2tcentury = Regex("Mitte (des )?" * rgpcentury, "i")
const rgx3tcentury = Regex("Ende (des )?" * rgpcentury, "i")

# half
const rgx1hcentury = Regex("(1\\.|erste) Hälfte +(des )?" * rgpcentury, "i")
const rgx2hcentury = Regex("(2\\.|zweite) Hälfte +(des )?" * rgpcentury, "i")

# between
const rgxbetween = Regex("zwischen " * rgpyear * " und " * rgpyear)

# early, late
const rgxearlycentury = Regex("frühes " * rgpcentury, "i")
const rgxlatecentury = Regex("spätes " * rgpcentury, "i")

# around, ...
const rgpmonth = "(Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember|Jan\\.|Feb\\.|Mrz\\.|Apr\\.|Jun\\.|Jul\\.|Aug\\.|Sep\\.|Okt\\.|Nov\\.|Dez\\.)"
const rgxbefore = Regex("(vor|bis|spätestens|spät\\.|v\\.)( [1-9][0-9]?\\.)? " * rgpmonth * "? ?" * rgpyear, "i")
# add 'circa'
const rgxaround = Regex("(um|circa|ca\\.|wahrscheinlich|wohl|etwa|evtl\\.) " * rgpyear, "i")
const rgxafter = Regex("(nach|frühestens|seit|ab) " * rgpyear, "i")

const rgxcentury = Regex("^ *" * rgpcentury)
const rgxyear = Regex("^( *|erwählt *)" * rgpyear)
const rgxyearfc = Regex("^( *|erwählt *)" * rgpyearfc)
const stripchars = ['†', '[', ']', ' ', '(', ')']
const rgxbelegt = r"belegt(.*)"

"""
    parsemaybe(s, Symbol::dir)

Parse `s` for an earliest or latest date. `dir` is `:upper` or `:lower`
"""
function parsemaybe(s, dir::Symbol)::Union{Missing, Int}
    if !(dir in [:lower, :upper])
        error("parameter dir must be :lower or :upper got ", dir)
    end

    year = missing
    if ismissing(s) || s == ""
        return year
    end

    # strip 'belegt'
    # prog: use replace instead
    rgm = match(rgxbelegt, s)
    if !isnothing(rgm)
        s = rgm[1]
    end

    # handle special cases
    s = strip(s, stripchars)

    if strip(s) == "?" return year end

    # turn of the century
    rgm = match(rgxtcentury, s)
    if !isnothing(rgm) && !isnothing(rgm[2])
        year = parse(Int, rgm[2])
        return year * 100
    end

    # quarter
    rgxq = [rgx1qcentury, rgx2qcentury, rgx3qcentury, rgx4qcentury]
    for (q, rgx) in enumerate(rgxq)
        rgm = match(rgx, s)
        if !isnothing(rgm) && !isnothing(rgm[3])
            century = parse(Int, rgm[3])
            if dir == :lower
                year = (century - 1) * 100 + (q - 1) * 25 + 1
                return year
            elseif dir == :upper
                year = (century - 1) * 100 + q * 25
                return year
            end
        end
    end

    # begin, middle, end
    rgxq = [rgx1tcentury, rgx2tcentury, rgx3tcentury]
    for (q, rgx) in enumerate(rgxq)
        rgm = match(rgx, s)
        if !isnothing(rgm) && !isnothing(rgm[3])
            century = parse(Int, rgm[2])
            if dir == :lower
                year = (century - 1) * 100 + (q - 1) * 33 + 1
                return year
            elseif dir == :upper
                year = (century - 1) * 100 + q * 33 + (q == 3 ? 1 : 0)
                return year
            end
        end
    end

    # half
    rgxq = [rgx1hcentury, rgx2hcentury]
    for (q, rgx) in enumerate(rgxq)
        rgm = match(rgx, s)
        if !isnothing(rgm) && !isnothing(rgm[3])
            century = parse(Int, rgm[3])
            if dir == :lower
                year = (century - 1) * 100 + (q - 1) * 50 + 1
                return year
            elseif dir == :upper
                year = (century - 1) * 100 + q * 50
                return year
            end
        end
    end

    # between
    rgm = match(rgxbetween, s)
    if !isnothing(rgm) && !isnothing(rgm[1]) && !isnothing(rgm[2])
        if dir == :lower
            year = parse(Int, rgm[1])
            return year
        elseif dir == :upper
            year = parse(Int, rgm[2])
            return year
        end
    end

    # early, late
    rgm = match(rgxearlycentury, s)
    if !isnothing(rgm) && !isnothing(rgm[1])
        century = parse(Int, rgm[1])
        if dir == :lower
            year = (century - 1) * 100 + 1
            return year
        elseif dir == :upper
            year = (century - 1) * 100 + 20
            return year
        end
    end

    rgm = match(rgxlatecentury, s)
    if !isnothing(rgm) && !isnothing(rgm[1])
        century = parse(Int, rgm[1])
        if dir == :lower
            year = century * 100 - 19
            return year
        elseif dir == :upper
            year = century * 100
            return year
        end
    end


    # before, around, after
    rgm = match(rgxbefore, s)
    if !isnothing(rgm)
        year = parse(Int, rgm[4])
        if dir == :lower
            year -= 50
        end
        return year
    end

    rgm = match(rgxafter, s)
    if !isnothing(rgm)
        year = parse(Int, rgm[2])
        if dir == :upper
            year += 50
        end
        return year
    end

    rgm = match(rgxaround, s)
    if !isnothing(rgm)
        year = parse(Int, rgm[2])
        if dir == :lower
            year -= 5
        elseif dir == :upper
            year += 5
        end
        return year
    end

    # century
    rgm = match(rgxcentury, s)
    if !isnothing(rgm) && !isnothing(rgm[1])
        century = parse(Int, rgm[1])
        if dir == :lower
            year = (century - 1) * 100 + 1
        elseif dir == :upper
            year = century * 100
        end
        return year
    end

    # plain year
    rgm = match(rgxyear, s)
    if !isnothing(rgm) && !isnothing(rgm[2])
        year = parse(Int, rgm[2])
        return year
    end

    # first century
    rgm = match(rgxyearfc, s)
    if !isnothing(rgm) && !isnothing(rgm[2])
        @info "First century date " s
        year = parse(Int, rgm[2])
        return year
    end

    # handle other special cases
    if strip(s) == "?" return year end

    ssb = strip(s, ['(', ')'])
    if ssb != s
        return parsemaybe(ssb, dir)
    end

    @warn "Could not parse " s
    return year

end

"""
    sqlvalue(data)

return quoted string or "NULL"
"""
function sqlvalue(data)::String
    value = ismissing(data) ? "NULL" : "'" * string(data) * "'"
    return value
end

"""
    filltable!(tablename::AbstractString, df::AbstractDataFrame; clear_table = false)::Int

read data from `df` and fill table `tablename`

The columns in `df` must correspond to the field names in the database table.
"""
function filltable!(tablename::AbstractString, df::AbstractDataFrame; clear_table = false)::Int
    global dbwiag

    if isnothing(dbwiag)
        setDBWIAG()
    end

    if clear_table
        DBInterface.execute(dbwiag, "DELETE FROM " * tablename)
    end

    # CSV returns String31 which is not properly handled by DBInterface
    function create_sql_row(row)
        sql_row = collect(row)
        for (i, e) in enumerate(sql_row)
            if typeof(e) <: AbstractString
                sql_row[i] = String(e)
            end
        end
        return sql_row
    end


    # fill database tables with chunks of data for performance reasons
    df_size = size(df, 1)
    chunk_size = 1000
    cols = join(names(df), ",")
    n_cols = length(names(df));
    placeholder_set = "(" * repeat("?,", n_cols - 1) * "?)"
    sql = "INSERT INTO " * tablename * "(" * cols * ")" *
        " VALUES " * repeat(placeholder_set * ",", chunk_size - 1) * placeholder_set
    stmt = DBInterface.prepare(dbwiag, sql)

    (n_chunk, n_remaining) = divrem(size(df, 1), chunk_size)
    count_line = 0
    msg_step = 10000
    chunk = Any[]
    for row in eachrow(view(df, 1:(n_chunk * chunk_size), :))
        push!(chunk, create_sql_row(row))
        count_line += 1
        if count_line % chunk_size == 0
            sql_chunk = vcat(reverse(chunk)...)
            DBInterface.execute(stmt, sql_chunk)
            chunk = Any[]
        end
        if count_line % msg_step == 0
            @info count_line
        end
    end
    DBInterface.close!(stmt)

    # rest
    n_cols = length(names(df));
    placeholder_set = "(" * repeat("?,", n_cols - 1) * "?)"
    sql = "INSERT INTO " * tablename * "(" * cols * ")" *
        " VALUES " * placeholder_set
    stmt = DBInterface.prepare(dbwiag, sql)

    chunk = Any[]
    for row in eachrow(view(df, df_size - n_remaining + 1:df_size, :))
        sql_row = create_sql_row(row)
        count_line += 1
        DBInterface.execute(stmt, sql_row)
        if count_line % msg_step == 0
            @info count_line
        end
    end
    DBInterface.close!(stmt)

    @info "Rows inserted: " * string(count_line)

    return count_line;
end

"""
    clean_up(df::AbstractDataFrame)

remove whitespaces
"""
function clean_up!(df::AbstractDataFrame)
    function pure(x)
        r = x
        if typeof(x)<:AbstractString
            r = strip(x)
            if x == ""
                r = missing
            end
        end
        return r
    end

    for col in names(df)
        df[!, col] = pure.(df[!, col])
    end

end

const WIKIPEDIA_PREFIX = "https://de.wikipedia.org/wiki/"

"""
    fix_Wikipedia_URL(s)

fix malformed Wikipedia URL, strip prefix
"""
function fix_Wikipedia_URL(url)
    rp = findfirst("http", url[5:end])
    if !isnothing(rp)
        url = url[rp.start + 4:end]
    end
    url = strip(url, '#')
    # strip prefix
    rp_prefix = findfirst(WIKIPEDIA_PREFIX, url)
    if !isnothing(rp_prefix)
        url = url[rp_prefix.stop + 1:end]
        println(url)
    end
    return url
end


end
