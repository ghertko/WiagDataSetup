module WiagDataSetup

using MySQL
using Infiltrator
using DataFrames
using CSV
using HTTP
using JSON
using Base
using Dates

function cp_valid(a, b)
    ismissing(a) && (a = b)
    ismissing(b) && (b = a)
    return a, b
end

dbwiag = nothing

function setDBWIAG(;pwd = missing, host = "127.0.0.1", user = "wiag", db = "wiag")
    global dbwiag
    if !isnothing(dbwiag)
        DBInterface.close!(dbwiag)
    end

    if ismissing(pwd)
        io_pwd = Base.getpass("Passwort für User " * user)
        pwd = readline(io_pwd)
        Base.shred!(io_pwd)
    end
    dbwiag = DBInterface.connect(MySQL.Connection, host, user, pwd, db = db)
end

"""
    sql_df(sql)

execute the commands in `sql` and return a DataFrame
"""
sql_df(sql) = DBInterface.execute(dbwiag, sql) |> DataFrame

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
                            tblonline::AbstractString = "",
                            colid::AbstractString = "")

    colnameofficeid = "id_office";

    global dbwiag
    if isnothing(dbwiag)
        setDBWIAG()
    end

    msg = 1000

    if isequal(tblonline, "")
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

    sqlml = "SELECT place_id, location_name, place_name, " *
        "location_begin_tpq as loc_start, location_end_tpq as loc_end" *
        " FROM " * tblmonasterylocation *
        " WHERE (location_name IS NOT NULL OR place_name IS NOT NULL) AND wiagid_monastery = ?"
    mlstmt = DBInterface.prepare(dbwiag, sqlml)

    sqlmlnn = "SELECT place_id, location_name, place_name, " *
        "location_begin_tpq as loc_start, location_end_tpq as loc_end" *
        " FROM " * tblmonasterylocation *
        " WHERE place_id IS NOT NULL AND location_name IS NULL AND wiagid_monastery = ?"
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
                places = vcat(ml[:, :location_name], ml[:, :place_name])
                places = collect(skipmissing(places))
            elseif nloc > 1
                mlfilter = filter([:loc_start, :loc_end] => ffilter, ml)
                places = vcat(mlfilter[:, :location_name], mlfilter[:, :place_name])
                places = collect(skipmissing(places))
            else
                # 2021-11-22 obsolete: we read monastery locations via HTTP from
                # GS-Klosterdatenbank and do not map places to monasteries
                # where no location name is given
                mlnn = DBInterface.execute(mlnonamestmt, [id_monastery]) |> DataFrame;
                # filter only if there is more than one alternative
                if size(mlnn, 1) > 1
                    mlfilter = filter([:loc_start, :loc_end] => ffilter, mlnn)
                ids_place = mlfilter[:, :place_id]
                else
                    ids_place = mlnn[:, :place_id]
                end
                if length(ids_place) > 0
                    @info ids_place
                    dfp = DBInterface.execute(plstmt, ids_place) |> DataFrame;
                    places = dfp[:, :place_name]
                else
                    @warn "No place found for office", id
                end
            end
        end
        if length(places) > 0
            location_show = places[1];
            DBInterface.execute(updstmt, [location_show, id])
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
            ismissing(sfni) ? sgni : (ismissing(sgni) ? sfni : sgni * " " * sfni),
            sgni * " " * sprefix * " " * sfni
        ]
        push!(csql, values)
    end

    # complete name
    pushcsql(gn, fn)

    if !ismissing(gn)
        cgn = split(gn, r" +")
        # more than one givenname (Hans Otto): write first part + familyname
        if length(cgn) > 1
            pushcsql(cgn[1], fn)
        end
    end

    # familyname variants
    if !ismissing(fnv) && strip(fnv) != ""
        cfnv = split(fnv, r",|; *")
        for fnve in cfnv
            pushcsql(gn, fnve)
            # more than one givenname (Hans Otto): write first part + familyname variant
            if length(cgn) > 1
                pushcsql(cgn[1], fnve)
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
const rgxtcentury = Regex("([1-9][0-9]?)\\.(/| oder )" * rgpcentury, "i")

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
const rgxca = Regex("(circa|ca\\.|wahrscheinlich|wohl|etwa|evtl\\.) " * rgpyear, "i")
const rgxaround = Regex("(um) " * rgpyear, "i")
const rgxafter = Regex("(nach|frühestens|seit|ab) " * rgpyear, "i")

const rgxcentury = Regex("^ *" * rgpcentury)
const rgxyear = Regex("^( *|erwählt |belegt )" * rgpyear)
const rgxyearfc = Regex("^( *|erwählt |belegt )" * rgpyearfc)
const stripchars = ['†', ' ']

"""
    parsemaybe(s, Symbol::dir)

Parse `s` for an earliest or latest date. `dir` is `:upper` or `:lower`
"""
function parsemaybe(s, dir::Symbol)::Union{Missing, Int}
    if !(dir in [:lower, :upper])
        error("parameter dir must be :lower or :upper got ", dir)
    end

    year = missing
    if ismissing(s)
        return year
    end

    # handle special cases
    s = strip(s, stripchars)

    if s == "?" || s == "" return year end

    # turn of the century
    rgm = match(rgxtcentury, s)
    if !isnothing(rgm) && !isnothing(rgm[1]) && !isnothing(rgm[3])
        if dir == :lower
            century = parse(Int, rgm[1])
            return year = (century - 1) * 100 + 1
        elseif dir == :upper
            century = parse(Int, rgm[3])
            return year = century * 100 - 1
        end
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

    rgm = match(rgxca, s)
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


    # handle other special cases
    if strip(s) == "?" return year end

    ssb = replace(s, r"\((.+)\)" => s"\1")
    if ssb != s
        return parsemaybe(ssb, dir)
    end

    # try to find a year
    rgxyearpx = r"([1-9][0-9][0-9][0-9]?)"
    rgm = match(rgxyearpx, s)
    if !isnothing(rgm) && !isnothing(rgm[1])
        @warn "Could only find year in " s
        year = parse(Int, rgm[1])
        return year
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

    # CSV returns String31 which is not properly handled by module DBInterface
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
    chunk_size = 30
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
            else
                # replace special whitespaces
                r = replace(r, "\u00A0" => "\u0020")
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
    if ismissing(url)
        return url
    end
    rp = findfirst("http", url[5:end])
    if !isnothing(rp)
        url = url[rp.start + 4:end]
    end
    url = strip(url, '#')
    # strip prefix
    rp_prefix = findfirst(WIKIPEDIA_PREFIX, url)
    if !isnothing(rp_prefix)
        url = url[rp_prefix.stop + 1:end]
        # println(url)
    end
    return url
end

"""
    expand_column(df::AbstractDataFrame, col; delim = ',')

Create an extra row for each entry in column `col`.
"""
function expand_column(df::AbstractDataFrame, col; delim = r", *")
    df_out = empty(df);
    for row in eachrow(df)
        c_col_new = strip.(split(row[col], delim))
        for value in c_col_new
            push!(df_out, row)
            r_new = df_out[end, col] = value
        end
    end
    return df_out
end

"""
    fill_name_lookup!(df_dst::AbstractDataFrame, df_src::AbstractDataFrame)

generate combinations of name variants in `df_src` and write them to `df_dst`

Column name for ID: `id` (input), `person_id` (output).
"""
function create_name_lookup(df::AbstractDataFrame)
    df_dst = DataFrame(person_id = Int[],
                       gn_fn = Union{String, Missing}[],
                       gn_prefix_fn = Union{String, Missing}[])

    function append_variants(id_person, variants)
        for row in variants
            push!(df_dst, (id_person, row[4], row[5]))
        end
    end

    for row in eachrow(df)
        id_person = row[:id]
        variants = makevariantsgn(row[[:givenname, :prefix_name, :familyname, :familyname_variant]]...)
        append_variants(id_person, variants)
        # given name variants
        gnv_raw = row[:givenname_variant]
        if !ismissing(gnv_raw)
            c_gnv = split(gnv_raw, r", *")
            for gnv in c_gnv
                variants = makevariantsgn(gnv, row[[:prefix_name, :familyname, :familyname_variant]]...)
                append_variants(id_person, variants)
            end
        end
    end

    return df_dst

end

url_gs_monasteries = "https://api.gs.sub.uni-goettingen.de/v1/monastery/"

"""
    get_gs_monasteries!(df_mon, df_mon_loc, df_id_mon)

request monasteries in `df_id_mon` and fill `df_mon`, `df_mon_loc`
"""
function get_gs_monasteries!(df_mon::AbstractDataFrame,
                             df_mon_loc::AbstractDataFrame,
                             c_id_mon)

    global url_gs_monasteries
    cols_mon = names(df_mon)
    cols_loc = names(df_mon_loc)

    empty_row = fill(missing, length(cols_mon) - 1)

    count = 0
    msg_count = 80;
    max_count = 100000;
    for id in c_id_mon
        data = get_gs_monastery_data(id)
        if !isnothing(data)
            # println(make_row(id, data))
            row = vcat([id], dict_to_array(data, cols_mon, ["wiagid"]))
            push!(df_mon, row)
            for loc in data["locations"]
                row_loc = vcat([id], dict_to_array(loc, cols_loc, ["wiagid_monastery"]))
                push!(df_mon_loc, row_loc)
            end
        else
            # add dummy data to avoid inconsistencies in the database structure
            push!(df_mon, vcat([id], empty_row))
        end

        count += 1
        if count % msg_count == 0
            @info count
        end
        if count > max_count; break; end

    end

    return nothing
end


"""
    get_gs_monastery_data(id)

request data for `id` from GS Klosterdatenbank

"""
function get_gs_monastery_data(id)
    url = url_gs_monasteries * string(id)
    r = HTTP.request("GET", url)
    status = r.status
    if status != 200
        @warn "Status " * string(status) * " for id " * string(id)
        return nothing
    end

    body = String(r.body)
    data = nothing
    try
        data = JSON.parse(body)
    catch(e)
        @warn "Could not parse " * body * " (id: " * string(id) * ")"
    end
    return data
end

"""
    dict_to_array(d, keys, skip_keys)

find values for `keys` in `d`
"""
function dict_to_array(d::AbstractDict, keys, skip_keys = String[])
    r = Any[]
    for k in keys
        if k in skip_keys
            continue
        end
        val = isnothing(d[k]) ? missing : strip(d[k])
        push!(r, val)
    end
    return r
end

struct Date_Regex
    rgx::Regex
    part::Int
    sort::Int
end

"""
    c_rgx_sort_cty

regular expressions for dates (century)

structure: regular expression, index of the relevant part, sort key
"""
c_rgx_sort_cty = [
    Date_Regex(rgxtcentury, 1, 850),
    Date_Regex(rgx1qcentury, 3, 530),
    Date_Regex(rgx2qcentury, 3, 560),
    Date_Regex(rgx3qcentury, 3, 580),
    Date_Regex(rgx4qcentury, 3, 595),
    Date_Regex(rgx1tcentury, 2, 500),
    Date_Regex(rgx2tcentury, 2, 570),
    Date_Regex(rgx3tcentury, 2, 594),
    Date_Regex(rgx1hcentury, 3, 550),
    Date_Regex(rgx2hcentury, 3, 590),
    Date_Regex(Regex("(wohl im )" * rgpcentury), 2, 810),
    Date_Regex(rgxearlycentury, 1, 555),
    Date_Regex(rgxlatecentury, 1, 593),
    Date_Regex(rgxcentury, 1, 800)
]

"""
    c_rgx_sort

regular expressions for dates

structure: regular expression, index of the relevant part, sort key
"""
c_rgx_sort = [
    Date_Regex(Regex("(kurz vor|bis kurz vor)([1-9][0-9]?\\.)? " * rgpmonth * "? ?" * rgpyear, "i"), 4, 105),
    Date_Regex(rgxbefore, 4, 100),
    Date_Regex(rgxaround, 2, 210),
    Date_Regex(rgxca, 2, 200),
    Date_Regex(Regex("(erstmals erwähnt) " * rgpyear, "i"), 2, 110),
    Date_Regex(Regex("(kurz nach|bald nach) " * rgpyear, "i"), 2, 303),
    Date_Regex(Regex("(Anfang der )" * rgpyear * "er Jahre"), 2, 305),
    Date_Regex(rgxafter, 2, 309),
    Date_Regex(Regex(rgpyear * "er Jahre"), 1, 310),
    Date_Regex(rgxyear, 2, 150)
]

"""
    parse_year_sort(s)

return a value for a year and a sort key

# Examples
"kurz vor 1200" -> 1200105183
"""
function parse_year_sort(s)
    year = 9000
    sort = 900
    day = 900
    # day_middle = 183

    # version for day specific dates
    # make_key(year, sort, day) = (year * 1000 + sort) * 1000 + day
    make_key(year, sort) = year * 1000 + sort
    key_not_found = make_key(9000, 900)

    if ismissing(s) || strip(s, stripchars) in ("", "?", "unbekannt")
        return make_key(year, sort)
    end

    rgm = match(rgxbetween, s)
    if !isnothing(rgm) && !isnothing(rgm[1]) && !isnothing(rgm[2])
        year_lower = parse(Int, rgm[1])
        year_upper = parse(Int, rgm[2])
        year = div(year_lower + year_upper, 2)
        if year > 3000
            @warn "year out of range in " s
            return key_not_found
        end
        return  make_key(year, 150)
    end

    for d in c_rgx_sort_cty
        rgm = match(d.rgx, s)
        if !isnothing(rgm) && !isnothing(rgm[d.part])
            century = parse(Int, rgm[d.part])
            year = century * 100 - 1;
            sort = d.sort
            if year > 3000
                @warn "year out of range in " s
                return key_not_found
            end
            return make_key(year, sort)
        end

    end

    for d in c_rgx_sort
        rgm = match(d.rgx, s)
        if !isnothing(rgm) && !isnothing(rgm[d.part])
            year = parse(Int, rgm[d.part])
            sort = d.sort
            if year > 3000
                @warn "year out of range in " s
                return key_not_found
            end
            return make_key(year, sort)
        end

    end

    @warn "could not parse " s
    return key_not_found
end

"""
    get_place(df_place, id, date_start, date_end)

2022-02-28 obsolete?
"""
function get_place(df_place, id, date_start, date_end)
    df_f_id = subset(df_place, :place_id => ByRow(isequal(id)))
    s = size(df_f_id, 1)
    if s == 0
        return missing
    elseif s == 1
        return df_f_id[1, :place_name]
    else
        date_start, date_end = cp_valid(date_start, date_end)
        date_cmp = div(date_start + date_end, 2)
        if !ismissing(date_centre)
            for row in eachrow(df_id)
                place_start = row[:num_date_begin]
                place_end = row[:num_date_end]
                place_name = row[:place_name]
                if (!ismissing(place_start) &&
                    !ismissing(place_end) &&
                    place_start < date_cmp < place_end)
                    return place_name
                elseif !ismissing(place_start) && place_start < date_cmp
                    return place_name
                elseif !ismissing(place_end) && date_cmp < place_end
                    return place_name
                end
            end
        else
            return df_f_id[1, :place_name]
        end
    end
    return missing
end

"""
    insert_sql(file_name, table_name, df::AbstractDataFrame; msg = 2000)

write content of `df` as an SQL INSERT statement for `table_name`
"""
function insert_sql(file_name, table_name, df::AbstractDataFrame; msg = 2000)
    file = open(file_name, "w")
    println(file, "LOCK TABLES `" * table_name * "` WRITE;")

    col_str = join(string.(names(df)), ", ")
    println(file, "INSERT INTO " * table_name * " (" * col_str * ") VALUES")
    size_df_1 = size(df, 1)
    i = 0
    for row in eachrow(df)
        val_line = "(" * join((val_sql(val) for val in row), ", ") * ")"
        print(file, val_line)
        i += 1
        if (i < size_df_1)
            println(file, ",")
        else
            println(file, ";")
        end
        if i % msg == 0
            @info "row " i
        end
    end

    println(file, "UNLOCK TABLES;")
    close(file)
    return i
end

"""
    update_sql(file_name, table_name, df::AbstractDataFrame; on = :id, msg = 2000)

write content of `df` as an SQL INSERT statement for `table_name`
"""
function update_sql(file_name, table_name, df::AbstractDataFrame; on = :id, msg = 2000)
    file = open(file_name, "w")
    println(file, "LOCK TABLES `" * table_name * "` WRITE;")

    c_col = filter(!isequal(on), Symbol.(names(df)))
    size_df_1 = size(df, 1)
    i = 0
    for row in eachrow(df)
        c_assignment = String[];
        for c in c_col
            a = string(c) * " = " * val_sql(row[c])
            push!(c_assignment, a)
        end
        c_token = ["UPDATE",
                   table_name,
                   "SET",
                   join(c_assignment, ", "),
                   "WHERE",
                   string(on) * " = " * val_sql(row[on])]
        println(file, join(c_token, " ") * ";")

        i += 1
        if i % msg == 0
            @info "row " i
        end
    end

    println(file, "UNLOCK TABLES;")
    close(file)
    return i
end

function val_sql(val::Real)
    return string(val)
end

function val_sql(val::AbstractString)
    return "'" * val * "'"
end

function val_sql(val::Missing)
    return "NULL"
end

const wiag_date_time_format = Dates.dateformat"yyyy-mm-dd HH:MM"
function val_sql(val::DateTime)
    return "'" * Dates.format(val, wiag_date_time_format) * "'"
end

const wiag_date_format = Dates.dateformat"yyyy-mm-dd"
function val_sql(val::Date)
    return "'" * Dates.format(val, wiag_date_format) * "'"
end


function val_sql(val::Any)
    return string(val)
end




end
