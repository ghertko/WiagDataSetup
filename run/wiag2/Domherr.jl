wds_path="../.."

cd(wds_path)

pwd()

out_path = "C:\\Users\\georg\\Documents\\projekte-doc\\WIAGweb2\\data_sql"

using Pkg

Pkg.activate(".")

using Revise

using WiagDataSetup

using MySQL, DataFrames

ENV["COLUMNS"] = 120

Wds = WiagDataSetup

Wds.setDBWIAG(user="georg", db="wiag2")

item_type_id = 5

data_path = "C:\\Users\\georg\\Documents\\projekte-doc\\WIAGweb2\\data\\domherren-2022-10-14"

table_name = "name_lookup";
sql = "DELETE FROM $(table_name)
WHERE person_id IN
(SELECT id FROM item WHERE item_type_id = $(item_type_id))";
DBInterface.execute(Wds.dbwiag, sql)

table_name = "familyname_variant";
sql = "DELETE FROM $(table_name)
WHERE person_id IN
(SELECT id FROM item WHERE item_type_id = $(item_type_id))";
DBInterface.execute(Wds.dbwiag, sql)

table_name = "givenname_variant";
sql = "DELETE FROM $(table_name)
WHERE person_id IN
(SELECT id FROM item WHERE item_type_id = $(item_type_id))";
DBInterface.execute(Wds.dbwiag, sql)

table_name = "url_external";
sql = "DELETE FROM $(table_name)
WHERE item_id IN
(SELECT id FROM item WHERE item_type_id = $(item_type_id))";
DBInterface.execute(Wds.dbwiag, sql)

table_name = "id_external";
sql = "DELETE FROM $(table_name)
WHERE item_id IN
(SELECT id FROM item WHERE item_type_id = $(item_type_id))";
DBInterface.execute(Wds.dbwiag, sql)

table_name = "person_role_property";
sql = "DELETE FROM $(table_name)
WHERE person_role_id IN
(SELECT id FROM person_role
WHERE person_id in (SELECT id from item WHERE item_type_id = $(item_type_id)))";
DBInterface.execute(Wds.dbwiag, sql)

table_name = "person_role";
sql = "DELETE FROM $(table_name)
WHERE person_id IN
(SELECT id FROM item WHERE item_type_id = $(item_type_id))";
DBInterface.execute(Wds.dbwiag, sql)

table_name = "person";
sql = "DELETE FROM $(table_name)
WHERE id IN
(SELECT id FROM item WHERE item_type_id = $(item_type_id))";
DBInterface.execute(Wds.dbwiag, sql)

table_name = "item_reference";
sql = "DELETE FROM $(table_name)
WHERE item_id IN
(SELECT id FROM item WHERE item_type_id = $(item_type_id))";
DBInterface.execute(Wds.dbwiag, sql)

table_name = "item";
sql = "DELETE FROM $(table_name)
WHERE item_type_id = $(item_type_id)";
DBInterface.execute(Wds.dbwiag, sql)

# Quelle Datenbank
sql = "SELECT * FROM domherr.tbl_reference"
df_vol = Wds.sql_df(sql);

using CSV

# Quelle CSV-Datei
filename = joinpath(data_path, "tbl_Reference.csv");
df_vol = CSV.read(filename, DataFrame);
size(df_vol)

Wds.clean_up!(df_vol);
size(df_vol)

names(df_vol)

columns = [
    :ID_Ref => :reference_id,
    :Literatur => :full_citation,
    :Verfasser => :author_editor,
    :OnlineRessource => :online_resource,
    :Kurztitel => :title_short,
    :Sort => :display_order,
    :ZitationGS => :gs_citation
]

df_vol_db = select(df_vol, columns);

insertcols!(df_vol_db,
    :item_type_id => item_type_id);

table_name = "reference_volume"
sql = "DELETE FROM $(table_name) WHERE item_type_id = $(item_type_id)";
DBInterface.execute(Wds.dbwiag, sql)

table_name = "reference_volume"
Wds.filltable!(table_name, df_vol_db, clear_table = false)

using CSV

# Quelle CSV-Datei
filename = joinpath(data_path, "tbl_domherren.csv")
df_cn = CSV.read(filename, DataFrame, stringtype=String);
size(df_cn)

# Quelle Datenbank
sql = "SELECT * FROM domherr.tbl_domherren";
df_cn = Wds.sql_df(sql);

Wds.clean_up!(df_cn)

unique(df_cn.Status)

first(df_cn, 5)

df_cn_ID_Bischof = dropmissing(df_cn, :WIAG_ID_Bischof);

df_cn_ID_Bischof[201:205, [:ID_Domherr, :Vorname, :Familienname, :WIAG_ID_Bischof, :Status]]

find_spurious(a) = !ismissing(a) && occursin("http", a)
df_cn_sp = subset(df_cn, :WIAG_ID_Bischof => ByRow(find_spurious));

# 2022-05-20
size(df_cn_sp)

df_cn_sp[:, [:ID_Domherr, :Vorname, :Familienname, :WIAG_ID_Bischof, :Status]]

filename_sp = "C:\\Users\\georg\\Documents\\projekte-doc\\WIAGweb2\\data\\WIAG_ID_Bischof_2022-07-26.csv"
CSV.write(filename_sp, df_cn_sp)

function get_WIAG_ID_Bischof(s)
    if ismissing(s)
        return s
    end
    rgx = r".*(WIAG-Pers-EPISCGatz-[0-9]+-[0-9]+).*"
    rgm = match(rgx, s)
    if !isnothing(rgm)
        return rgm[1]
    else
        @warn s
    end
    return missing
end


transform!(df_cn_sp, :WIAG_ID_Bischof => ByRow(get_WIAG_ID_Bischof) => :WIAG_ID_Bischof);

df_cn_sp[:, [:ID_Domherr, :Vorname, :Familienname, :WIAG_ID_Bischof, :Status]]

transform!(df_cn, :WIAG_ID_Bischof => ByRow(get_WIAG_ID_Bischof) => :WIAG_ID_Bischof);

columns = [
    :ID_Domherr => :id_in_source,
    :Eintrag_Quelle => :item_in_source,
    :fertig => :fertig,
    :Status => :Status,
]

df_item = select(df_cn, columns);

insertcols!(df_item, :item_type_id => item_type_id);

df_item[201:205, :]

function map_status(x)
    if typeof(x) == Int
        if x == 1
            return "fertig"
        else
            return missing
        end
    elseif getproperty(x, :bits) > 0
        return "fertig"
    else
        return missing
    end
end


transform!(df_item, :fertig => ByRow(map_status) => :edit_status);

map_online(x) = (!ismissing(x) && x == "online") ? 1 : 0

transform!(df_item, :Status => ByRow(map_online) => :is_online);

using Dates

date_time_now = Dates.format(now(), Dates.dateformat"yyyy-mm-dd HH:MM")

user_id_georg = 23

insertcols!(df_item,
    :created_by => user_id_georg,
    :date_created => date_time_now,
    :changed_by => user_id_georg,
    :date_changed => date_time_now
);

function make_id_public(id::Real)
    id_public_key = "Pers-CANON"
    num_id_length = 5
    return "WIAG-" * id_public_key * "-" * lpad(id, num_id_length, '0') * "-001"
end

make_id_public(3650)

transform!(df_item, :id_in_source => ByRow(make_id_public) => :id_public);

names(df_item)

sum(df_item[!, :is_online])

findfirst(df_item[!, :is_online] .== 1)

df_item[8:12, [:id_in_source, :item_type_id, :id_public, :Status, :is_online, :date_changed, :changed_by]]

item_type_id

table_name = "item";
sql = "DELETE FROM $(table_name) WHERE item_type_id = $(item_type_id)"
DBInterface.execute(Wds.dbwiag, sql)

columns = [
    :id_in_source => :id_in_source,
 :item_in_source => :item_in_source,
 :Status => :edit_status,
 :item_type_id => :item_type_id,
 :is_online => :is_online,
 :created_by => :created_by,
 :date_created => :date_created,
 :changed_by => :changed_by,
 :date_changed => :date_changed,
 :id_public => :id_public
]

table_name = "item";
Wds.filltable!(table_name, select(df_item, columns), clear_table = false)

sql = "SELECT id, id_in_source FROM ITEM where item_type_id = ($item_type_id)"
df_idx = DBInterface.execute(Wds.dbwiag, sql) |> DataFrame;

transform!(df_cn, :ID_Domherr => ByRow(string) => :id_in_source);

df_cn_db = innerjoin(df_idx, df_cn, on = :id_in_source);

one_not_missing(x, y) = !(ismissing(x) && ismissing(y))

size(df_cn_db, 1)

df_cn_db = subset(df_cn_db, [:Vorname, :Familienname] => ByRow(one_not_missing));

size(df_cn_db, 1)

parse_date_birth(s) = Wds.parsemaybe(s, :lower)

parse_date_death(s) = Wds.parsemaybe(s, :upper)

transform!(df_cn_db, :Geburtsdatum => ByRow(parse_date_birth) => :num_date_birth);

transform!(df_cn_db, :Sterbedatum => ByRow(parse_date_death) => :num_date_death);

sql = "SELECT id as religious_order_id, abbreviation FROM religious_order";
df_rel_order = Wds.sql_df(sql);

df_cn_db = leftjoin(df_cn_db, df_rel_order, on = :Orden => :abbreviation, matchmissing = :notequal);

a_not_b(a, b) = !ismissing(a) && ismissing(b)

df_cn_no_match = subset(df_cn_db, [:Orden, :religious_order_id] => ByRow(a_not_b));

size(df_cn_no_match)

insertcols!(df_cn_db,
    :item_type_id => item_type_id,
);

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

Wds.filltable!("person", select(df_cn_db, columns); clear_table=false)

df_cn_db_Bemerkung_Red = dropmissing(df_cn_db, :Bemerkung_Red);

size(df_cn_db_Bemerkung_Red)

df_cn_db_Bemerkung_Red[16:20, [:id, :Vorname, :Familienname, :Bemerkung_Red]]

df_cn_db.len_bem = (x -> ismissing(x) ? 0 : length(x)).(df_cn_db.Bemerkung_Red);

findmax(identity, df_cn_db.len_bem)

size(df_cn_db)

df_cn_db[22362, [:id, :Vorname, :Familienname, :Bemerkung_Red, :len_bem]]

df_cn_order = dropmissing(df_cn_db, :Orden);

size(df_cn_order)

names(df_cn_order)

df_cn_order[!, :join_field] .= lowercase.(df_cn_order[!, :Orden]);

sql = "SELECT * FROM religious_order WHERE abbreviation IS NOT NULL";
df_order_db = Wds.sql_df(sql);

df_order_db[!, :join_field] .= lowercase.(df_order_db[!, :abbreviation]);

df_cn_order_id = leftjoin(df_cn_order,
    select(df_order_db, :id => :religious_order_id, :join_field),
    on = :join_field);

size(df_cn_order_id)

filename = joinpath(out_path, "cn_person_update.sql");
table_name = "person";
Wds.update_sql(filename, table_name, select(df_cn_order_id, :id, :religious_order_id), on = :id);

table = "item_property_type"
sql = "SELECT * from $(table)"
df_ipt = Wds.sql_df(sql);

df_ipt

df_cn_db[51:55, [:id, :id_in_source, :Vorname, :Familienname, :AkademischerTitel]]

columns = [
    :id => :id,
    :Vorname => :givenname,
    :Familienname => :familyname,
    :AkademischerTitel => :academic_title
]

df_academic_title = dropmissing(select(df_cn_db, columns), :academic_title);

size(df_academic_title)

rec_academic_title = filter(:name => isequal("academic_title"), df_ipt)
academic_title_id = rec_academic_title[1, :item_type_id]

insertcols!(df_academic_title, :name => "academic_title", :property_type_id => academic_title_id);

columns = [
    :id => :item_id,
    :name => :name, # redundant
    :academic_title => :value,
    :property_type_id => :property_type_id
]

table_name = "item_property"
Wds.filltable!(table_name, select(df_academic_title, columns))

columns = [
    :id => :item_id,
    :Seite_Quelle => :page,
    :ID_Quelle => :id_in_reference,
    :Quelle => :reference_id,
    :Merged_Into => :merged_into_id,
    :Status => :status,
]

df_cn_ref = select(df_cn_db, columns);

df_cn_ref[!, :item_type_id] .= item_type_id;

size(df_cn_ref, 1)

dropmissing!(df_cn_ref, :reference_id);

size(df_cn_ref)

sql = "DELETE FROM item_reference WHERE item_type_id = $(item_type_id)"
DBInterface.execute(Wds.dbwiag, sql)

columns = [
    :item_id => :item_id,
    :page => :page,
    :id_in_reference => :id_in_reference,
    :item_type_id => :item_type_id,
    :reference_id => :reference_id,
]

table_name = "item_reference";
Wds.filltable!(table_name, select(df_cn_ref, columns), clear_table = false)

is_ref2(merged_into_id, status) = !ismissing(merged_into_id) && isequal(status, "merged")

df_cn_ref2 = subset(df_cn_ref, [:merged_into_id, :status] => ByRow(is_ref2));

lookupItemId = Dict(Pair.(df_cn_db.id_in_source ,df_cn_db.id));

getItemId(id_in_source) = get(lookupItemId, string(id_in_source), missing)

transform!(df_cn_ref2, :merged_into_id => ByRow(getItemId) => :item_id);

size(df_cn_ref2, 1)

count(ismissing, df_cn_ref.item_id)

columns = [
    :item_id,
    :page,
    :id_in_reference,
    :item_type_id,
    :reference_id,
]

table_name = "item_reference";
Wds.filltable!(table_name, select(df_cn_ref2, columns), clear_table = false)

subset(df_cn_ref2, :merged_into_id => ByRow(isequal(10939)))

using CSV, DataFrames

file_name = joinpath(data_path, "tbl_urls.csv")
df_url = CSV.read(file_name, DataFrame, delim=";");

Wds.clean_up!(df_url);

names(df_url)

id_offset = 1000;
transform!(df_url, :url_type => ByRow(x -> x + id_offset) => :authority_id);

sql = "SELECT id, id_in_source
FROM item
WHERE item_type_id = $(item_type_id)"
df_idx = Wds.sql_df(sql);

transform!(df_url, :domherr_id => ByRow(string) => :id_in_source);

df_url_idx = leftjoin(df_url, df_idx, on = :id_in_source);

size(df_url_idx, 1), count(ismissing, df_url_idx.id)

df_url_idx_mg = subset(df_url_idx, :id => ByRow(ismissing));

size(df_url_idx_mg)

df_url_idx_mg[:, [:id, :id_in_source, :url_type, :url_value]]

dropmissing!(df_url_idx, :id);

dropmissing!(df_url_idx, :url_value);

unique(df_url_idx.url_type)

table_name = "url_external";
sql = "DELETE FROM $(table_name)
WHERE item_id IN (SELECT id FROM item WHERE item_type_id = $(item_type_id))";
DBInterface.execute(Wds.dbwiag, sql)

columns = [
    :id => :item_id,
    :authority_id => :authority_id,
    :url_value => :value,
    :note => :note,
]

table_name = "url_external"
Wds.filltable!(table_name, select(df_url_idx, columns))

item_type_id = 5

sql = "DELETE FROM id_external " *
"WHERE item_id IN (SELECT id FROM item WHERE item_type_id = $(item_type_id))"
DBInterface.execute(Wds.dbwiag, sql)

sql

sql = "SELECT id, url_name_formatter FROM authority " *
"WHERE id IN (1, 2, 3, 4, 5, 200)";
Wds.sql_df(sql)

sql = "SELECT id, is_online, id_in_source FROM item WHERE item_type_id = $(item_type_id)"
df_idx = Wds.sql_df(sql);

size(df_idx, 1)

using CSV

col_types = Dict(
    :GND_ID => String,
    :GSN_ID => String,
    :VIAF_ID => String,
    :Wikidata_ID => String,
    :URL_Wikipedia => String,
)

# Quelle CSV-Datei
filename = joinpath(data_path, "tbl_domherren.csv")
df_cn = CSV.read(filename, DataFrame, types=col_types);

# Quelle: Datenbank
table_name = "domherr.tbl_domherren";
sql = "SELECT ID_Domherr, Vorname, Familienname, " *
"VIAF_ID, GND_ID, Wikidata_ID, GSN_ID, WIAG_ID_Bischof, URL_Wikipedia " *
"FROM $(table_name)"
df_cn = Wds.sql_df(sql);

Wds.clean_up!(df_cn)

transform!(df_cn, :ID_Domherr => ByRow(string) => :id_in_source);

df_cn = innerjoin(df_idx, df_cn, on = :id_in_source);

names(df_cn)

authority_id_viaf = 4 # siehe oben

df_cn_viaf = dropmissing(df_cn, :VIAF_ID)
df_cn_viaf[!, :authority_id] .= authority_id_viaf;

columns = [
    :id => :item_id,
    :authority_id => :authority_id,
    :VIAF_ID => :value,
]

df_ins = select(df_cn_viaf, columns);

transform!(df_ins, :value => ByRow(string) => :value);

df_ins[303:307, :]

Wds.filltable!("id_external", df_ins)

authority_id_gnd = 1 # siehe oben

df_cn_gnd = dropmissing(df_cn, :GND_ID)
df_cn_gnd[!, :authority_id] .= authority_id_gnd;

df_ins = select(df_cn_gnd, :id => :item_id, :authority_id, :GND_ID => :value);

df_ins[303:307, :]

Wds.filltable!("id_external", df_ins)

authority_id_wd = 2 # siehe oben

df_cn_wd = dropmissing(df_cn, :Wikidata_ID)
filter!(:Wikidata_ID => !isequal("ergaenzen"), df_cn_wd);

df_cn_wd[!, :authority_id] .= authority_id_wd;

df_ins = select(df_cn_wd, :id => :item_id, :authority_id, :Wikidata_ID => :value);

df_ins[300:307, :]

Wds.filltable!("id_external", df_ins)

is_incomplete(s) = !ismissing(s) && occursin(r".*-00$", s)

idx_b = findall(is_incomplete, df_cn.GSN_ID)

df_cn[idx_b, [:ID_Domherr, :Vorname, :Familienname, :GSN_ID]]

fix_gsn_id(s) = ismissing(s) ? s : replace(s, r"-00$" =>"-001")

transform!(df_cn, :GSN_ID => ByRow(fix_gsn_id) => :GSN_ID);

transform!(df_cn, :WIAG_ID_Bischof => ByRow(get_WIAG_ID_Bischof) => :WIAG_ID_Bischof);

table_name = "item";
item_type_bishop = 4;
sql = "SELECT i.id as ep, i.id_public, e1.value AS ep_gsn
FROM $(table_name) AS i
JOIN id_external AS e1 ON e1.item_id = i.id AND e1.authority_id = 200
WHERE i.item_type_id = $(item_type_bishop) AND i.is_online";
df_ep = Wds.sql_df(sql);

df_ep[71:75, :]

df_cn[91:95, [:ID_Domherr, :Vorname, :Familienname, :GSN_ID]]

size(df_cn, 1), count(!ismissing, df_cn.GSN_ID)

df_cn_ep = leftjoin(df_cn, df_ep, on = :WIAG_ID_Bischof => :id_public, matchmissing = :notequal);

for row in eachrow(df_cn_ep)
    if ismissing(row[:GSN_ID]) && !ismissing(row[:ep_gsn])
        row[:GSN_ID] = row[:ep_gsn]
    end
end

size(df_cn_ep, 1), count(!ismissing, df_cn_ep.GSN_ID)

authority_id_gsn = 200 # siehe oben

df_cn_gsn = dropmissing(df_cn_ep, :GSN_ID)
df_cn_gsn[!, :authority_id] .= authority_id_gsn;

gso_db = "gso_in_202210"

# gsn_gsn wird erzeugt aus gsn, siehe notebook Domherr-GS
sql = "SELECT id_new AS gs_gsn_id, nummer, nummer_new
FROM $(gso_db).gsn_gsn";
df_gsn_gsn = Wds.sql_df(sql);

df_ins_gsn = innerjoin(df_cn_gsn, df_gsn_gsn, on = :GSN_ID => :nummer);

df_new_gsn = subset(df_ins_gsn, [:nummer_new, :GSN_ID] => ByRow(!isequal));

size(df_new_gsn)

df_new_gsn[:, [:id, :ID_Domherr, :Vorname, :Familienname, :GSN_ID, :nummer_new, :Status]]

columns = [
    :id => :id,
    :is_online => :is_online,
    :ID_Domherr => :ID_Domherr,
    :Vorname => :Vorname,
    :Familienname => :Familienname,
    :GSN_ID => :GSN_ID,
    :nummer_new => :GSN_ID_neu,
]

out_path = "C:\\Users\\georg\\Documents\\projekte-doc\\WIAGweb2\\data";
filename = joinpath(out_path, "domherren_gsn_neu_2022-07-26.csv");
CSV.write(filename, select(df_new_gsn, columns), delim = ";")

df_ins = select(df_ins_gsn, :id => :item_id, :authority_id, :nummer_new => :value);

df_ins[321:325, :]

Wds.filltable!("id_external", df_ins)

authority_id_wiag = 5 # siehe oben

df_cn_wiag = dropmissing(df_cn, :WIAG_ID_Bischof)
df_cn_wiag[!, :authority_id] .= authority_id_wiag;

df_ins = select(df_cn_wiag, :id => :item_id, :authority_id, :WIAG_ID_Bischof => :value);

size(df_ins)

Wds.filltable!("id_external", df_ins)

authority_id_wp = 3 # siehe oben

df_cn_wp = dropmissing(df_cn, :URL_Wikipedia)
df_cn_wp[!, :authority_id] .= authority_id_wp;

df_ins = select(df_cn_wp, :id => :item_id, :authority_id, :URL_Wikipedia => :value);

url_m = "#https://de.wikipedia.org/wiki/Ruprecht_von_der_Pfalz_%28Freising%29#https://de.wikipedia.org/wiki/Ruprecht_von_der_Pfalz_%28Freising%29#"
Wds.fix_Wikipedia_URL(url_m)

transform!(df_ins, :value => ByRow(Wds.fix_Wikipedia_URL) => :value);

df_ins[300:304, :]

using CSV

file_wp = "C:\\Users\\georg\\Documents\\tmp\\bishops_url_wikidata.csv"

CSV.write(file_wp, df_ins)

Wds.filltable!("id_external", df_ins)

using MySQL, DataFrames

# Quelle CSV-Datei
filename = joinpath(data_path, "tbl_domherren.csv");
df_cn = CSV.read(filename, DataFrame);

# Quelle Datenbank
table_name = "domherr.tbl_domherren"
sql = "SELECT ID_Domherr, Vorname, Familienname, Vorname_Variante, Familienname_Variante " *
"FROM $(table_name)"
df_cn = Wds.sql_df(sql);

Wds.clean_up!(df_cn)

df_fnv = dropmissing(df_cn, :Familienname_Variante);

size(df_fnv, 1)

df_fnv[201:207, :]

df_exp = Wds.expand_column(df_fnv, :Familienname_Variante, delim = ",");

size(df_exp, 1)

df_exp[151:157, :]

transform!(df_exp, :ID_Domherr => ByRow(string) => :id_in_source);

df_ins = innerjoin(df_idx, df_exp, on = :id_in_source);

size(df_ins, 1)

table_name = "familyname_variant"
Wds.filltable!(table_name, select(df_ins, :id => :person_id, :Familienname_Variante => :name))

df_gnv = dropmissing(df_cn, :Vorname_Variante);

size(df_gnv, 1)

df_gnv[201:207, :]

df_exp = Wds.expand_column(df_gnv, :Vorname_Variante, delim = r", *");

size(df_exp, 1)

df_exp[151:157, :]

transform!(df_exp, :ID_Domherr => ByRow(string) => :id_in_source);

df_ins = innerjoin(df_idx, df_exp, on = :id_in_source);

size(df_ins, 1)

table_dst = "givenname_variant"
Wds.filltable!(table_dst, select(df_ins, :id => :person_id, :Vorname_Variante => :name))

Wds.makevariantsgn("Klaus Walter", "von der", "Vogelweide", "Vollmer, Liber")

df_src = DataFrame(
    id = [1077, 31, 32, 33, 34, 35, 36],
    givenname = ["Dietricus", "Albert", "Hans Otto", "Wilhelm", "Kurt Georg", "Otto", "Otto"],
    prefix_name = [missing, "von", "zu", "von", missing, "auf der", missing],
    familyname = [missing, "Häusler", "Oberhof", "Berg", missing, "Scholle", "Weiß"],
    givenname_variant = [missing, missing, "Johann Otto", "Guido, Guillaume", "Konrad", missing, "Odilon"],
    familyname_variant = [missing, "Hauser, Schad", missing, "Berge", missing, missing, "Blanc"]
    )

df_nl = Wds.create_name_lookup(df_src) # schaut gut aus

table_name = "item"
sql = "SELECT id, id_in_source FROM $(table_name) where item_type_id = ($item_type_id)"
df_idx = Wds.sql_df(sql);

filename = joinpath(data_path, "tbl_domherren.csv")
df_src = CSV.read(filename, DataFrame);

Wds.clean_up!(df_src);

columns = [
    :ID_Domherr => :ID_Domherr,
    :Vorname => :givenname,
    :Praefix => :prefix_name,
    :Familienname => :familyname,
    :Vorname_Variante => :givenname_variant,
    :Familienname_Variante => :familyname_variant
]

select!(df_src, columns);

# Alternativ: Daten aus der Datenbank auslesen, wenn sie sich von Access aus dahin exportieren lassen.
table_src = "domherr.tbl_domherren"
sql = "SELECT ID_Domherr, " *
"Vorname AS givenname, " *
"Praefix AS prefix_name, " *
"Familienname AS familyname," *
"Vorname_Variante AS givenname_variant, " *
"Familienname_Variante AS familyname_variant " *
"FROM $(table_src)"
df_src = Wds.sql_df(sql);

Wds.clean_up!(df_src)

one_not_missing(x, y) = !(ismissing(x) && ismissing(y))

filter!([:givenname, :familyname] => one_not_missing, df_src);

transform!(df_src, :ID_Domherr => ByRow(string) => :id_in_source);

size(df_src)

names(df_src)

df_idx[101:105, :]

df_nl_in = innerjoin(df_idx, df_src, on = :id_in_source);

size(df_nl_in)

df_nl = Wds.create_name_lookup(df_nl_in);

size(df_nl)

df_nl_empty = filter(:gn_fn => ismissing, df_nl);

size(df_nl_empty)

df_nl[2301:2312, :]

table_name = "name_lookup"
sql = "DELETE FROM $(table_name)
WHERE person_id IN (SELECT id FROM item WHERE item_type_id = $(item_type_id))";
DBInterface.execute(Wds.dbwiag, sql)

table_name = "name_lookup"
Wds.filltable!(table_name, df_nl)

table_name = "item";
item_type_dh = 5;
sql = "SELECT id as person_id, id as person_id_canon
FROM $(table_name) WHERE item_type_id = $(item_type_dh) AND is_online";
df_item_cn_dh = Wds.sql_df(sql);

size(df_item_cn_dh)

table_name = "canon_lookup";
Wds.filltable!(table_name, df_item_cn_dh);

sql = "SELECT p.id as person_id, givenname, familyname, e.value AS id_external, i.item_type_id
FROM person AS p
JOIN item AS i ON p.id = i.id
JOIN id_external AS e ON i.id = e.item_id
JOIN authority AS a ON e.authority_id = a.id
WHERE a.id = 5
AND i.is_online";
df_cn_episc = Wds.sql_df(sql);

size(df_cn_episc)

names(df_cn_episc)

table_name = "item";
type_id_episc = 4;
sql = "SELECT id as id_ep, id_public FROM $(table_name) WHERE item_type_id = $(type_id_episc)";
df_item_episc = Wds.sql_df(sql);

df_cn_episc = innerjoin(df_cn_episc, df_item_episc, on = :id_external => :id_public);

size(df_cn_episc)

names(df_cn_episc)

columns = [
    :person_id => :person_id,
    :id_ep => :person_id_canon
]

table_name = "canon_lookup"
Wds.filltable!(table_name, select(df_cn_episc, columns))

id_src = "(" * join(df_cn_episc.person_id, ",") * ")";

sql = "DELETE FROM canon_lookup
WHERE person_id = person_id_canon
AND person_id in $(id_src)";
DBInterface.execute(Wds.dbwiag, sql)
