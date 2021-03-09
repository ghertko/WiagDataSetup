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
    fillera(tblera::AbstractString, tblperson::AbstractString, tbloffice::AbstractString, colnameid::AbstractString, datereference=false)::Int

Compute earliest and latest date for each person, identified by `colnameid` and `colnameidinoffice`.

Take fields `date_hist_first` and `date_hist_last` into account if `datereference` is set to `true`.
"""
function fillera(tblera::AbstractString,
                 tblperson::AbstractString,
                 tbloffice::AbstractString,
                 colnameid = "id",
                 colnameidinoffice = "id_person";
                 datereference = false)::Int
    global dbwiag
    if isnothing(dbwiag)
        error("There is no valid database connection. Use `setDBWIAG'.")
    end

    DBInterface.execute(dbwiag, "DELETE FROM " * tblera);

    if datereference
        sqlselect = "SELECT " * colnameid * " as idperson, " *
            " date_birth, date_death, date_hist_first, date_hist_last " *
            " FROM " * tblperson;
    else
        sqlselect = "SELECT " * colnameid * " as idperson, date_birth, date_death " * " FROM " * tblperson;
    end

    dfperson = DBInterface.execute(dbwiag, sqlselect) |> DataFrame;

    tblid = 0;

    # get office data
    sqlselect = "SELECT " * colnameidinoffice * ", date_start, date_end " * " FROM " * tbloffice
    dfoffice = DBInterface.execute(dbwiag, sqlselect) |> DataFrame

    csqlvalues = String[]
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

        sqlerastart = erastart == Inf ? "NULL" : "'" * string(erastart) * "'"
        sqleraend = eraend == -Inf ? "NULL" : "'" * string(eraend) * "'"

        # if !ismissing(erastartdb) && !(typeof(erastartdb) == Int)
        #     println("start: ", erastartdb)
        # end
        # if !ismissing(eraenddb) && !(typeof(eraenddb) == Int)
        #     println("end: ", eraenddb)
        # end

        push!(csqlvalues, "('" * string(idperson) * "', " * sqlerastart * ", " * sqleraend * ")")
        tblid += 1
    end

    sqlvalues = join(csqlvalues, ", ")
    DBInterface.execute(dbwiag, "INSERT INTO " * tblera * " VALUES " * sqlvalues)

    # println(sqlvalues);

    return tblid
end

"""
    fillofficedate(tblofficedate::AbstractString, tbloffice::AbstractString, colnameid::AbstractString)::Int

Extract dates as an integer values.
"""
function fillofficedate(tblofficedate::AbstractString,
                        tbloffice::AbstractString,
                        colnameid::AbstractString = "id")::Int

    global dbwiag
    if isnothing(dbwiag)
        setDBWIAG()
    end

    sql = "SELECT " * colnameid * ", date_start, date_end FROM " * tbloffice
    dfoffice = DBInterface.execute(dbwiag, sql) |> DataFrame;

    csqlvalues = String[]
    tblid = 0;
    for row in eachrow(dfoffice)
        id, date_start, date_end = row

        numdate_start = parsemaybe(date_start, :lower)
        numdate_end = parsemaybe(date_end, :upper)
        if ismissing(numdate_end)
            numdate_end = parsemaybe(date_start, :upper)
        end        

        push!(csqlvalues, "(" * id * ", " * numdate_start * ", " * numdate_end * ")")

        tblid += 1
        # if tblid > 25 break end
    end

    DBInterface.execute(dbwiag, "DELETE FROM " * tblofficedate);

    sqlvalues = join(csqlvalues, ", ")
    DBInterface.execute(dbwiag, "INSERT INTO " * tblofficedate * " VALUES " * sqlvalues)

    return tblid
end

"""
    fillnamelookup(tablename::AbstractString)::Int

Fill `tablename` with combinations of givenname and familyname and their variants.
"""
function fillnamelookup(tbllookup::AbstractString,
                        tblperson::AbstractString,
                        colnameid::AbstractString = "id")::Int
    msg = 200
    if isnothing(dbwiag)
        error("There is no valid database connection. Use `setDBWIAG'.")
    end

    DBInterface.execute(dbwiag, "DELETE FROM " * tbllookup);

    dfperson = DBInterface.execute(dbwiag,
                                   "SELECT " * colnameid * " as id_person, " *
                                   "givenname, prefix_name, familyname, givenname_variant, familyname_variant " *
                                   "FROM " * tblperson * " person") |> DataFrame;

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
            println("write row: ", imsg)
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
        s = strip(s[poslabel + 1:end])
    end
    s = replace(s, "'" => "''")
    s = "'" * s * "'"
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
# quarter
const rgpcentury = "([1-9][0-9]?)\\. (Jahrh|Jh)"
const rgpyear = "([1-9][0-9][0-9]+)"
const rgpyearfc = "([1-9][0-9]+)"

const rgx1qcentury = Regex("(1\\.|erstes) Viertel (des )?" * rgpcentury, "i")
const rgx2qcentury = Regex("(2\\.|zweites) Viertel (des )?" * rgpcentury, "i")
const rgx3qcentury = Regex("(3\\.|drittes) Viertel (des )?" * rgpcentury, "i")
const rgx4qcentury = Regex("(4\\.|viertes) Viertel (des )?" * rgpcentury, "i")

# begin, middle end
const rgx1tcentury = Regex("Anfang (des )?" * rgpcentury, "i")
const rgx2tcentury = Regex("Mitte (des )?" * rgpcentury, "i")
const rgx3tcentury = Regex("Ende (des )?" * rgpcentury, "i")

# half
const rgx1hcentury = Regex("(1\\.|erste) Hälfte (des )?" * rgpcentury, "i")
const rgx2hcentury = Regex("(2\\.|zweite) Hälfte (des )?" * rgpcentury, "i")

# between
const rgxbetween = Regex("zwischen " * rgpyear * " und " * rgpyear)

# early, late
const rgxearlycentury = Regex("frühes " * rgpcentury, "i")
const rgxlatecentury = Regex("spätes " * rgpcentury, "i")

# around, ...
const rgpmonth = "(Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember|Jan\\.|Feb\\.|Mrz\\.|Apr\\.|Jun\\.|Jul\\.|Aug\\.|Sep\\.|Okt\\.|Nov\\.|Dez\\.)"
const rgxbefore = Regex("(vor|bis|spätestens) " * rgpmonth * "? ?" * rgpyear, "i")
const rgxaround = Regex("(um|ca\\.|wahrscheinlich) " * rgpyear, "i")
const rgxafter = Regex("(nach|frühestens|seit) " * rgpyear, "i")

const rgxcentury = Regex("^ *" * rgpcentury)
const rgxyear = Regex("^ *" * rgpyear)
const rgxyearfc = Regex("^ *" * rgpyearfc)

"""
    parsemaybe(s, Symbol::dir)

Parse `s` for an earliest or latest date.
"""
function parsemaybe(s, dir::Symbol)::Union{Missing, Int}
    if !(dir in [:lower, :upper])
        error("parameter dir must be :lower or :upper got ", dir)
    end

    year = missing
    if ismissing(s)
        return year
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
        year = parse(Int, rgm[3])
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
    if !isnothing(rgm) && !isnothing(rgm[1])
        year = parse(Int, rgm[1])
        return year
    end

    # first century
    rgm = match(rgxyearfc, s)
    if !isnothing(rgm) && !isnothing(rgm[1])
        @info "First century date " s
        year = parse(Int, rgm[1])
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
