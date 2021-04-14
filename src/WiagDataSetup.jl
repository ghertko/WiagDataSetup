module WiagDataSetup

using MySQL
using Infiltrator
using DataFrames

dbwiag = nothing

function setDBWIAG(pwd = missing, host = "127.0.0.1", user = "wiag", db = "wiag")
    global dbwiag
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
    fillera(tblera::AbstractString, tblperson::AbstractString, tbloffice::AbstractString, colnameid::AbstractString, datereference=false, checkisonline = true)::Int

Compute earliest and latest date for each person, identified by `colnameid` and `colnameidinoffice`.

Take fields `date_hist_first` and `date_hist_last` into account if `datereference` is set to `true`.
"""
function fillera(tblera::AbstractString,
                 tblperson::AbstractString,
                 tbloffice::AbstractString,
                 colnameid = "id",
                 colnameidinoffice = "id_person";
                 datereference = false,
                 checkisonline = false)::Int
    global dbwiag
    if isnothing(dbwiag)
        error("There is no valid database connection. Use `setDBWIAG'.")
    end

    msg = 1000

    DBInterface.execute(dbwiag, "DELETE FROM " * tblera);

    if datereference
        sqlselect = "SELECT " * colnameid * " as idperson, " *
            " date_birth, date_death, date_hist_first, date_hist_last " *
            " FROM " * tblperson
    else
        sqlselect = "SELECT " * colnameid * " as idperson, date_birth, date_death " *
            " FROM " * tblperson
    end

    if checkisonline
        sqlselect *= " WHERE status = 'online'"
    end


    dfperson = DBInterface.execute(dbwiag, sqlselect) |> DataFrame;

    tblid = 0;

    # get office data
    sqlselect = "SELECT " * colnameidinoffice * ", date_start, date_end " * " FROM " * tbloffice
    dfoffice = DBInterface.execute(dbwiag, sqlselect) |> DataFrame

    insstmt = DBInterface.prepare(dbwiag, "INSERT INTO " * tblera * " VALUES (?, ?, ?)")
    for row in eachrow(dfperson)
        erastart = Inf
        eraend = -Inf

        idperson, datebirth, datedeath = row[[:idperson, :date_birth, :date_death]]

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

        if datereference
            datehistfirst, datehistlast = row[[:date_hist_first, :date_hist_last]]
            vcand = parsemaybe(datehistfirst, :lower)
            if !ismissing(vcand) && vcand < erastart
                erastart = vcand
            end

            vcand = parsemaybe(datehistlast, :upper)
            if ismissing(vcand)
                # we may have a date like "13. Jhd"
                vcand = parsemaybe(datehistfirst, :upper)
            end

            if !ismissing(vcand) && vcand > eraend
                eraend = vcand
            end
        end

        # println(wiagid, " ", typeof(dfoffice[:wiagid_person]))
        ixperson = dfoffice[:, colnameidinoffice] .== string(idperson)

        dfofficeperson = dfoffice[ixperson, :];
        for oc in eachrow(dfofficeperson)
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

        # it is a bit slower to do call the database in each step, but needs less code
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
    fillofficedate(tblofficedate::AbstractString, tbloffice::AbstractString, colnameid::AbstractString; checkisonline = false)::Int

Extract dates as integer values.
"""
function fillofficedate(tblofficedate::AbstractString,
                        tbloffice::AbstractString;
                        colnameid::AbstractString = "id",
                        checkisonline = false,
                        colnameidperson = "wiagid",
                        colnameidinoffice = "wiagid_person",
                        tblperson = "person")::Int

    global dbwiag
    if isnothing(dbwiag)
        setDBWIAG()
    end

    msg = 1000

    sql = "SELECT " * colnameid * ", date_start, date_end, id_monastery FROM " * tbloffice

    if checkisonline
        sql *= " WHERE " * colnameidinoffice *
            " IN (SELECT " * colnameidperson * " FROM " * tblperson *
            " WHERE status = 'online')"
    end

    dfoffice = DBInterface.execute(dbwiag, sql) |> DataFrame;

    tblid = 0;
    DBInterface.execute(dbwiag, "DELETE FROM " * tblofficedate);
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

"""
    fillofficelocation(tbloffice::AbstractString, tblofficedate::Abstractstring, tblmonasterylocation::AbstractString, tblplace::AbstractString, colnameid::AbstractString)::Int

Find locations for offices that are related to a monastery.
"""
function fillofficelocation(tbloffice::AbstractString,
                            tblofficedate::AbstractString,
                            tblmonasterylocation::AbstractString,
                            tblplace::AbstractString,
                            colnameid::AbstractString = "id")

    colnameofficeid = "id_office";

    global dbwiag
    if isnothing(dbwiag)
        setDBWIAG()
    end

    msg = 1000

    sqlo = "SELECT " * colnameid * ", id_monastery, location, d.date_start, d.date_end" *
        " FROM " * tbloffice * " as o" *
        " LEFT JOIN " * tblofficedate * " as d ON o." * colnameid * " = d." * colnameofficeid

    dfoffice = DBInterface.execute(dbwiag, sqlo) |> DataFrame;

    sqlml = "SELECT place_id, location_name, location_begin_tpq as loc_start, location_end_tpq as loc_end" *
        " FROM " * tblmonasterylocation * " WHERE location_name IS NOT NULL AND wiagid_monastery = ?"
    mlstmt = DBInterface.prepare(dbwiag, sqlml)

    sqlmlnn = "SELECT place_id, location_name, location_begin_tpq as loc_start, location_end_tpq as loc_end" *
        " FROM " * tblmonasterylocation * " WHERE location_name IS NULL AND wiagid_monastery = ?"
    mlnonamestmt = DBInterface.prepare(dbwiag, sqlmlnn)

    sqlp = "SELECT place_name FROM " * tblplace * " WHERE id_places IN (?)"
    plstmt = DBInterface.prepare(dbwiag, sqlp)

    sqlupd = "UPDATE " * tbloffice * " SET location = ? WHERE id = ?"
    updstmt = DBInterface.prepare(dbwiag, sqlupd)

    ntest = 30
    ir = 0
    for row in eachrow(dfoffice)
        places = String[];
        id, id_monastery, location, date_start, date_end = row
        if !ismissing(location) && location != "" || ismissing(id_monastery) || id_monastery == ""
            continue
        end

        ffilter(loc_start, loc_end) = filterlocbydate(loc_start, loc_end, date_start, date_end)

        ml = DBInterface.execute(mlstmt, [id_monastery]) |> DataFrame;
        nloc = size(ml, 1)
        if nloc == 1
            push!(places, ml[1, :location_name])
        elseif nloc > 1
            mlfilter = filter([:loc_start, :loc_end] => ffilter, ml)
            places = mlfilter[:, :location_name]
        else
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
        if length(places) > 0
            DBInterface.execute(updstmt, [places[1], id])
            ir += 1
            if ir % msg == 0
                @info ir
            end
        end

    end

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
    sqlstring(s::AbstractString)::AbstractString

remove labels in data fields ("Taufname: Karl") and escape apostrophes
"""
function sqlstring(s::AbstractString)::AbstractString
    poslabel = findfirst(':', s)
    if !isnothing(poslabel)
        s = s[poslabel + 1:end]
    end
    s = replace(s, "'" => "''")
    s = "'" * strip(s) * "'"
    return s
end


function fillnamelookupgn(id_person, gn, prefix, fn, fnv)
    csql = String[]

    isnull(s) = ismissing(s) || s == "NULL"
    function pushcsql(gni, fni)
        sgni = isnull(gni) ? "NULL" : sqlstring(gni)
        sfni = isnull(fni) ? "NULL" : sqlstring(fni)
        prefixi = isnull(prefix) ? "NULL" : sqlstring(prefix)
        if !ismissing(id_person)
            id_person_sql = sqlstring(id_person)
            values = "(" * "NULL, " * id_person_sql * ", " * sgni * ", " * prefixi * ", " * sfni * ")"
            push!(csql, values)
        else
            @warn "Missing ID for ", gni
        end
    end

    pushcsql(gn, fn)
    cgn = split(gn);
    # more than one givenname -> write a version with the first givenname only
    if length(cgn) > 1
        pushcsql(cgn[1], fn)
    end

    # familyname variants
    if !ismissing(fnv) && strip(fnv) != ""
        cfnv = split(fnv, r", *")
        for fnve in cfnv
            pushcsql(gn, fnve)
            # more than one givenname
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

"""
    parsemaybe(s, Symbol::dir)

Parse `s` for an earliest or latest date.
"""
function parsemaybe(s, dir::Symbol)::Union{Missing, Int}
    if !(dir in [:lower, :upper])
        error("parameter dir must be :lower or :upper got ", dir)
    end

    year = missing
    if ismissing(s) || s == ""
        return year
    end

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
            year = century * 100 - 20
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
            return year
        elseif dir == :upper
            year = century * 100
            return year
        end
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



end
