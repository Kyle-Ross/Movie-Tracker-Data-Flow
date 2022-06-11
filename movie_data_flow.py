#|||||||||||||||||
#|||PREPARATION|||
#|||||||||||||||||

#import imdbpy for getting imdb data
import imdb
#import regex for URL parser
import re
#import gspread
import gspread
#import datetime for recording timing of entries
from datetime import datetime
#import library for postgresql connection
import psycopg2
#initialise the imdb class
ia = imdb.IMDb()
#for datetime object conversion at gspread step
from datetime import datetime
#import json to deal with datetime serialisation issue, iterprets any issues as strings
import json
#import custom function to generate color palettes from image URLs
import color_palette_vals_from_url as col_pal

#Set location of service account JSON and create object
sa = gspread.service_account(
    filename=r"C:\Users\kylec\AppData\Local\Programs\Python\Python39\Scripts\movie_data_flow\API Keys\media-tracker-dataflow-63dcb9efef74.json")

# Defining db connection variables
DB_HOST = "localhost"
DB_NAME = "media_dataflow"
DB_USER = "postgres"
DB_PASS = "pizzadataman66#" # Placeholder password for Git commit

#Create workbook objects
wb_box_off = sa.open("box_office")
wb_mov_comp = sa.open("movie_companies")
wb_mov_list_val = sa.open("movie_list_values")
wb_mov_ppl = sa.open("movie_people")
wb_pm_vals = sa.open("per_movie_values")
wb_media_trkr = sa.open("media_tracker")
wb_mov_unq = sa.open("movies_unique")
wb_mov_watch_vals = sa.open("movie_per_watch_values")
wb_mov_write_log = sa.open("mov_write_log")

#Create worksheet objects
wks_box_off = wb_box_off.worksheet("box_office")
wks_mov_comp = wb_mov_comp.worksheet("movie_companies")
wks_mov_list_val = wb_mov_list_val.worksheet("movie_list_values")
wks_mov_ppl = wb_mov_ppl.worksheet("movie_people")
wks_pm_vals = wb_pm_vals.worksheet("per_movie_values")
wks_media_trkr = wb_media_trkr.worksheet("Main")
wks_mov_unq = wb_mov_unq.worksheet("movies_unique")
wks_mov_watch_vals = wb_mov_watch_vals.worksheet("movie_per_watch_values")
wks_mov_write_log = wb_mov_write_log.worksheet("mov_write_log")

#|||||||||||||||||||||
#|||SMALL FUNCTIONS|||
#|||||||||||||||||||||

#function to write a log to the log database
def write_log(logtext1,logtext2):
    media_db_conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)  # connection object
    log_cur = media_db_conn.cursor()  # cursor obj

    log_cur.execute("INSERT INTO mov_write_log (log_name,action_name) VALUES(%s,%s)", (logtext1,logtext2))

    log_cur.close()  # Closing cursor obj
    media_db_conn.commit()  # Committing changes
    media_db_conn.close()  # Closing Connection

#Function to grab the imdbid from the url
def imdb_url_parse(url):
    reg_ex = r"(?<=[/][a-zA-Z][a-zA-Z])\d+(?=[/])"
    match_obj = re.search(reg_ex,url)
    return match_obj.group()

#|||||||||||||||||||||||||
#|||BUILDING MOVIE DICT|||
#|||||||||||||||||||||||||

#Build a dictionary of movie info starting with the imdbid and using the movie object
def get_data(imdb_id, print_me = False):
    #Prepare dictionary
    movie_dict = {"per_movie_values":0, #dictionary
                  "movie_people": {}, #Dict with list of id/name lists
                  "movie_list_values":[], # List of lists of type / value pairs
                  "movie_companies":{}, #Dict with list of id/name lists lots of duplicated data
                  "box_office":[]} #List of lists with type, string and number pairs

    #Get movie object from imdb
    movie_obj = ia.get_movie(imdb_id)

    #Place "per_movie_values" data in dictionary

    def get_match(dict_key,regx):
        my_val = movie_obj.get(dict_key, None)
        try:
            if my_val == None:
                return None
            else:
                return re.search(regx, my_val).group()
        except:
            return my_val

    movie_dict["per_movie_values"] = {
        "localized_title" : movie_obj.get('localized title',None),
        "original_air_date" : get_match('original air date',r".+(?=[\s][(])"),
        "original_air_country": get_match('original air date',r"(?<=[(]).+(?=[)])"),
        "rating" : movie_obj.get('rating',None),
        "votes" : movie_obj.get('votes',None),
        "imdbID" : movie_obj.get('imdbID',None),
        "plot_outline" : movie_obj.get('plot outline',None),
        "title" : movie_obj.get('title',None),
        "year" : movie_obj.get('year',None),
        "kind" : movie_obj.get('kind',None),
        "cover_url" : movie_obj.get('cover url',None),
        "full_size_cover_url" : movie_obj.get('full-size cover url',None)}

    #Place "movie_people" data in dictionary

    #function to create new people lists and add them to the main dictionary
    def person_list(person_type,add_to):
        try:
            add_to[person_type.replace(" ","_")] = [[x.personID,x.get('name',None)] for x in movie_obj[person_type]]
        except KeyError:
            add_to[person_type.replace(" ","_")] = [None,None]

    #Running the function for all people types
    person_list("cast", movie_dict["movie_people"])
    person_list("directors", movie_dict["movie_people"])
    person_list("writers", movie_dict["movie_people"])
    person_list("producers", movie_dict["movie_people"])
    person_list("writer", movie_dict["movie_people"])
    person_list("director", movie_dict["movie_people"])

    #Place "movie_companies" data in dictionary

    #function to create new company lists and add them to the main dictionary
    def company_list(company_type,add_to):
        def key_expt_list_comp():
            try:
                [[x.companyID, x.get('name', None)] for x in movie_obj[company_type]]
            except KeyError:
                return [None,None]
        add_to[company_type.replace(" ","_")] = key_expt_list_comp()

    #Running the function for all company types
    company_list("production companies", movie_dict["movie_companies"])
    company_list("distributors", movie_dict["movie_companies"])

    #function to add list type data to main dictionary
    def list_types(field_name,add_to):
        try:
            for x in [[field_name.replace(" ","_"),x] for x in movie_obj[field_name]]:
                add_to.append(x)
        except KeyError:
            add_to.append([None,None])

    #Running function add data to main dict
    list_types("genres",movie_dict["movie_list_values"])
    list_types("runtimes",movie_dict["movie_list_values"])
    list_types("countries",movie_dict["movie_list_values"])
    list_types("country codes",movie_dict["movie_list_values"])
    list_types("language codes",movie_dict["movie_list_values"])
    list_types("certificates",movie_dict["movie_list_values"])
    list_types("languages",movie_dict["movie_list_values"])
    list_types("akas",movie_dict["movie_list_values"])

    #function to add box office data to main dictionary
    #if not box office data, returns none values
    def box_office(add_to):
        combo_list = []
        if "box office" in movie_obj:
            for keys, values in movie_obj["box office"].items():
                try:
                    #Attempts to get the $ number out of the text while excluding dates
                    regy_ex = r"[$][0-9,]+(?=[\s])"
                    just_money = re.search(regy_ex,values).group()
                except AttributeError:
                    just_money = values
                #Appends a version of the text with all non-numeric values removed
                combo_list.append([keys.replace(" ","_"),values,"".join([x for x in just_money if x.isnumeric()])])
            for x in combo_list:
                add_to.append(x)
        else:
            add_to.append([None,None,None])

    #Adding everything from the box office dict to the main dict
    box_office(movie_dict['box_office'])

    #returning or printing the dictionary
    if print_me == False:
        return movie_dict
    else:
        print(movie_dict)

#|||||||||||||||||||||||||
#|||LIST TRANSFORMATION|||
#|||||||||||||||||||||||||

#Further Transforming Data
def write_mov_data(mov_dict,write = False):

    #Append per_movie_values
    pmv_dict = mov_dict['per_movie_values']
    pmv_output_list = [
        str(datetime.now()),
        pmv_dict['imdbID'],
        pmv_dict['localized_title'],
        pmv_dict['original_air_date'],
        pmv_dict['original_air_country'],
        pmv_dict['rating'],
        pmv_dict['votes'],
        pmv_dict['plot_outline'],
        pmv_dict['title'],
        pmv_dict['year'],
        pmv_dict['kind'],
        pmv_dict['cover_url'],
        pmv_dict['full_size_cover_url']]

    #Append the dominant color from the cover image
    pmv_output_list.append(col_pal.img_color_from_url(pmv_dict['full_size_cover_url'], rgbhex = 'hex', my_opt = "dominant"))

    #Create list of 5 palette colors from the image
    palette_list = col_pal.img_color_from_url(pmv_dict['full_size_cover_url'], rgbhex = 'hex', my_opt = "palette")

    #Append those 5 palette colors to the list above
    pmv_output_list.append(palette_list[0])
    pmv_output_list.append(palette_list[1])
    pmv_output_list.append(palette_list[2])
    pmv_output_list.append(palette_list[3])
    pmv_output_list.append(palette_list[4])

    #col_pal

    #Append movie_people values
    ppl_dict = mov_dict['movie_people']
    ppl_append_listv1 = []
    for type, lists_list in ppl_dict.items():
        for index, lists in enumerate(lists_list):
            if lists == None or lists == [None,None]:
                continue
            else:
                ppl_append_listv1.append(
                    [str(datetime.now()),
                    pmv_dict['imdbID'],
                    type,
                    lists_list[index][0],
                    lists_list[index][1]])

    #function to remove duplicate lists and skip blanks
    def de_dupe_list(a_list,blank_index1, blank_index2):
        new_list = []
        for my_list in a_list:
            if my_list[blank_index1] == None and my_list[blank_index2] == None:
                continue
            elif my_list not in new_list:
                new_list.append(my_list)
        return new_list

    #removes duplicates and blanks
    ppl_output_list = de_dupe_list(ppl_append_listv1,3,4)

    #Append movie_list_values
    mov_list_listv1 = mov_dict['movie_list_values']
    mov_list_listv2 = []
    for my_list in mov_list_listv1:
        mov_list_listv2.append([
            str(datetime.now()),
                pmv_dict['imdbID'],
                my_list[0],
                my_list[1]])

    #remove duplicate valyes
    mov_list_output = de_dupe_list(mov_list_listv2,2,3)

    #seperate out values in string for certificates and akas
    cert_regex = r"(^([^:]+):)"
    before_brac_regx = r".+(?=[\s][(])"
    in_brac_regx = r"(?<=[(]).+(?=[)])"
    for my_list in mov_list_output:
        if my_list[2] == "certificates":
            try:
                my_list.append(my_list[3].replace(re.search(cert_regex, my_list[3]).group(), ""))
            except AttributeError:
                my_list.append("No Match on Regex")
            try:
                my_list[3] = re.search(cert_regex, my_list[3]).group()[:-1]
            except:
                continue
        elif my_list[2] == "akas":
            try:
                my_list.append(re.search(in_brac_regx, my_list[3]).group())
            except AttributeError:
                my_list.append("No Match on Regex")
            try:
                my_list[3] = re.search(before_brac_regx, my_list[3]).group()
            except:
                continue
        else:
            my_list.append("Does Not Apply") #Added to avoid index error during db write


    #Append movie_companies values, skips if no list available (happens for most)
    comp_dict = mov_dict['movie_companies']
    comp_append_listv1 = []
    for type, lists_list in comp_dict.items():
        if lists_list == [None,None]:
            continue
        elif lists_list == None:
            continue
        else:
            for index, lists in enumerate(lists_list):
                if lists == [None, None]:
                    continue
                elif lists == None:
                    continue
                else:
                    comp_append_listv1.append(
                        [str(datetime.now()),
                        pmv_dict['imdbID'],
                        type,
                        lists_list[index][0],
                        lists_list[index][1]])

    #removes duplicates and blanks
    comp_output_list = de_dupe_list(comp_append_listv1,3,4)

    #Append box_office values
    box_list1 = mov_dict['box_office']
    box_list2 = []
    for my_list in box_list1:
        box_list2.append([
            str(datetime.now()),
                pmv_dict['imdbID'],
                my_list[0],
                my_list[1],
                my_list[2]])

    #remove duplicate valyes
    box_output_list = de_dupe_list(box_list2,2,3)

    #write directly or return as tuple of lists
    if write == False: #the default value
        return pmv_output_list, ppl_output_list, mov_list_output, comp_output_list, box_output_list
    else:
        wks_pm_vals.append_row(pmv_output_list)
        wks_mov_ppl.append_rows(ppl_output_list)
        wks_mov_list_val.append_rows(mov_list_output)
        wks_mov_comp.append_rows(comp_output_list)
        wks_box_off.append_rows(box_output_list)

#|||||||||||||||||||||||||||||||||||||||||||||||||
#|||COMBINING MULTIPLE RESULTS FOR BATCH UPLOAD|||
#|||||||||||||||||||||||||||||||||||||||||||||||||

def data_combiner(mov_id_list,write = False):
    #Lists for combines
    pmv_combo,ppl_combo,movl_combo,comp_combo,box_combo = [],[],[],[],[]

    #function to combine lists with above
    def list_combiner(destination, list_lists):
        for x in list_lists:
            destination.append(x)

    for id in mov_id_list:
        #Creates the tuple containing the variable outputs for each id
        list_tuple = write_mov_data(get_data(id))
        #unpack variables
        pmv_output_list, ppl_output_list, mov_list_output, comp_output_list, box_output_list = list_tuple
        #combine
        pmv_combo.append(pmv_output_list)
        list_combiner(ppl_combo, ppl_output_list)
        list_combiner(movl_combo, mov_list_output)
        list_combiner(comp_combo, comp_output_list)
        list_combiner(box_combo, box_output_list)

    #write to gsheet or return tuple of lists
    if write == False: #the default value
        return pmv_combo,ppl_combo,movl_combo,comp_combo,box_combo
    else:
        wks_pm_vals.append_rows(pmv_combo)
        wks_mov_ppl.append_rows(ppl_combo)
        wks_mov_list_val.append_rows(movl_combo)
        wks_mov_comp.append_rows(comp_combo)
        wks_box_off.append_rows(box_combo)

#||||||||||||||||||||||||||||
#|||VAR / LIST DEFINITIONS|||
#||||||||||||||||||||||||||||

#Transforming media_tracker for use in functions below
media_tracker_all = wks_media_trkr.get_all_values()

row_trim = []
for my_list in media_tracker_all:
    row_trim.append(my_list[2:8])

media_tracker_movies = []
#get just movies, also removes the headers
for my_list in row_trim:
    if my_list[2] == "Movie":
        media_tracker_movies.append(my_list)

#get ids from urls and add on the end
media_tracker_movies_ids = []
for my_list in media_tracker_movies:
    my_list.append(imdb_url_parse(my_list[5]))
    media_tracker_movies_ids.append(my_list)

#get unique movie data
media_tracker_movies_unq = []
for my_list in media_tracker_movies_ids:
    if my_list[-1] not in [x[-1] for x in media_tracker_movies_unq]:
        media_tracker_movies_unq.append(my_list)

#Creates "new_ids_list" var which contains list of movie ids that are in the tracker but not in the database
media_db_conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST) #connection object
db_unq_ids_cur = media_db_conn.cursor() #cursor obj
wks_unique_ids = [x[-1] for x in media_tracker_movies_unq] #list of unique ids from media tracker
db_unq_ids_cur.execute("SELECT movie_id FROM movies_unique") #Get list of unique ids from the database
media_db_conn.commit() # Committing changes to the cur obj
db_raw_list = db_unq_ids_cur # Setting those cur values into a var
db_unique_ids = [x[0] for x in db_raw_list] #Getting just the id, removing nested list
new_ids_list = [] #Builts this list
for my_id in wks_unique_ids:
    if int(my_id) not in db_unique_ids:
        new_ids_list.append(my_id)
db_unq_ids_cur.close() #Closing cursor obj
media_db_conn.commit() #Committing any remaining changes
media_db_conn.close() #Closing Connection

#|||||||||||||||||||
#|||WRITING TO DB|||
#|||||||||||||||||||

#Tables have built in constraints to support duplicates and updates

#Writing updates based on a list of movie ids to the database
def write_to_db(movie_ids):
    #Creating the datasets in with a tuple output
    combined_data_tuples = data_combiner(movie_ids)

    #Unpacking variables from the tuple
    pmv_combo,ppl_combo,movl_combo,comp_combo,box_combo = combined_data_tuples

    #Creating connection object
    media_db_conn = psycopg2.connect(dbname = DB_NAME,user = DB_USER, password = DB_PASS, host = DB_HOST)

    #Creating cursor
    mdb_cur = media_db_conn.cursor()

    #Adding / updating to the per_movie_values table
    for my_list in media_tracker_movies_unq:
        mdb_cur.execute(
            "UPDATE movies_unique\
            SET\
                given_media_name = %s,\
                url = %s,\
                data_source = %s\
            WHERE\
                movie_id = %s;",
            (my_list[1],my_list[5], "IMDB",my_list[-1]))

        mdb_cur.execute(
            "INSERT INTO movies_unique (given_media_name, movie_id, data_source, url)\
            VALUES(%s,%s,%s,%s)\
            ON CONFLICT (movie_id) DO NOTHING", #tries to update everything, but ignores rows where the primary key unique constraint conflicts (i.e no duplicates)
            (my_list[1],my_list[-1],"IMDB",my_list[5]))

    #Adding / updating data to the movie_per_watch_values table
    #Does not update rows where the date was incorrect - will have to do that manually!
    for my_list in media_tracker_movies_ids:
        mdb_cur.execute(
            "UPDATE movie_per_watch_values\
            SET\
                given_media_name = %s,\
                platform = %s,\
                short_comment = %s\
            WHERE\
                movie_id = %s AND datetime_finished = %s;",
            (my_list[1],my_list[3],my_list[4],my_list[-1],my_list[0]))

        mdb_cur.execute(
            "INSERT INTO movie_per_watch_values (datetime_finished, given_media_name, platform, short_comment,movie_id)\
            VALUES(%s,%s,%s,%s,%s)\
            ON CONFLICT (datetime_finished,movie_id) DO NOTHING", #Only inserts new rows that are unique per these two columns which are a unique key constraint of the table
            (my_list[0],my_list[1],my_list[3],my_list[4],my_list[-1]))

    # Adding / updating data to the per_movie_values table
    for my_list in pmv_combo:
        mdb_cur.execute(
            "UPDATE per_movie_values\
            SET\
                entry_datetime = %s,\
                localized_title = %s,\
                original_air_date = %s,\
                original_air_country = %s,\
                rating = %s,\
                votes = %s,\
                plot_outline = %s,\
                title = %s,\
                year = %s,\
                kind = %s,\
                cover_url = %s,\
                full_size_cover_url = %s,\
                cover_hex_dominant = %s,\
                cover_hex_palette1 = %s,\
                cover_hex_palette2 = %s,\
                cover_hex_palette3 = %s,\
                cover_hex_palette4 = %s,\
                cover_hex_palette5 = %s\
            WHERE\
                movie_id = %s;",
            (my_list[0],my_list[2],my_list[3],my_list[4],my_list[5],my_list[6],my_list[7],my_list[8],my_list[9],my_list[10],my_list[11],my_list[12],my_list[13],my_list[14],my_list[15],my_list[16],my_list[17],my_list[18],my_list[1]))

        mdb_cur.execute(
            "INSERT INTO per_movie_values (entry_datetime,movie_id,localized_title,original_air_date,original_air_country,rating,votes,plot_outline,title,year,kind,cover_url,full_size_cover_url,cover_hex_dominant,cover_hex_palette1,cover_hex_palette2,cover_hex_palette3,cover_hex_palette4,cover_hex_palette5)\
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)\
            ON CONFLICT (movie_id) DO NOTHING",
            # Only inserts new rows that are unique to  movie_id which is a unique key constraint of the table
            (my_list[0], my_list[1], my_list[2], my_list[3], my_list[4],my_list[5],my_list[6],my_list[7],my_list[8],my_list[9],my_list[10],my_list[11],my_list[12],my_list[13],my_list[14],my_list[15],my_list[16],my_list[17],my_list[18]))

    # Adding / updating data to the movie_people table
    for my_list in ppl_combo:
        mdb_cur.execute(
            "UPDATE movie_people\
            SET\
                entry_datetime = %s,\
                type = %s,\
                person_name = %s\
            WHERE\
                movie_id = %s AND person_id = %s",
            (my_list[0],my_list[2],my_list[4],my_list[1],my_list[3]))

        mdb_cur.execute(
            "INSERT INTO movie_people (entry_datetime,movie_id,type,person_id,person_name)\
            VALUES(%s,%s,%s,%s,%s)\
            ON CONFLICT (movie_id,person_id) DO NOTHING",
            # Only inserts new rows that are unique to movie_id & person_id which is a unique key constraint of the table
            (my_list[0],my_list[1],my_list[2],my_list[3],my_list[4]))

    #Adding data to the movie_list_values table
    #This table has no update behaviour as there is no meaningful way to set a unique constraint for these values
    for my_list in movl_combo:
        mdb_cur.execute(
            "INSERT INTO movie_list_values (entry_datetime,movie_id,type,value_string1,value_string2)\
            VALUES(%s,%s,%s,%s,%s)\
            ON CONFLICT ON CONSTRAINT movie_list_values_un DO NOTHING",
            # Only inserts new rows that are unique to all columns which is a unique key constraint of the table
            (my_list[0],my_list[1],my_list[2],my_list[3],my_list[4]))

    # Adding / updating data to the movie_companies table - most movies don't provide this data
    for my_list in comp_combo:
        mdb_cur.execute(
            "UPDATE movie_companies\
            SET\
                entry_datetime = %s,\
                type = %s,\
                company_name = %s\
            WHERE\
                movie_id = %s AND company_id = %s",
            (my_list[0],my_list[2],my_list[4],my_list[1],my_list[3]))

        mdb_cur.execute(
            "INSERT INTO movie_companies (entry_datetime,movie_id,type,company_id,company_name)\
            VALUES(%s,%s,%s,%s,%s)\
            ON CONFLICT (movie_id,company_id) DO NOTHING",
            # Only inserts new rows that are unique to movie_id & company_id which is a unique key constraint of the table
            (my_list[0],my_list[1],my_list[2],my_list[3],my_list[4]))

    # Adding / updating data to the box_office table - some movies don't provide this data
    for my_list in box_combo:
        mdb_cur.execute(
            "UPDATE box_office\
            SET\
                entry_datetime = %s,\
                string = %s,\
                numbers_extract = %s\
            WHERE\
                movie_id = %s AND type = %s",
            (my_list[0], my_list[3], my_list[4], my_list[1], my_list[2]))

        mdb_cur.execute(
            "INSERT INTO box_office (entry_datetime,movie_id,type,string,numbers_extract)\
            VALUES(%s,%s,%s,%s,%s)\
            ON CONFLICT (movie_id,type) DO NOTHING",
            # Only inserts new rows that are unique to movie_id & type which is a unique key constraint of the table
            (my_list[0], my_list[1], my_list[2], my_list[3], my_list[4]))

    #Closing the cursor
    mdb_cur.close()

    #Committing the changes
    media_db_conn.commit()

    #Closing Connection
    media_db_conn.close()

#|||||||||||||||||||||||||||||||||||||||
#|||UPDATE NEW OR UPDATE ALL FUNCTION|||
#|||||||||||||||||||||||||||||||||||||||

#Grab and write updates to the db, either only new stuff or update everything with most recent values
def update_db_method(new_or_all):
    #Creating connection object
    media_db_conn = psycopg2.connect(dbname = DB_NAME,user = DB_USER, password = DB_PASS, host = DB_HOST)
    #Creating cursor, with ability to refer to columns by name
    mdb_cur = media_db_conn.cursor()

    #Get list of unique ids from the google sheet
    wks_unq_ids = [x[-1] for x in media_tracker_movies_unq]

    #Get list of unique ids from the database
    mdb_cur.execute("SELECT movie_id FROM movies_unique")
    #Committing changes
    media_db_conn.commit()
    #Setting cursor list of lists to variable
    raw_list = mdb_cur
    #Getting just the id
    db_unq_ids = [x[0] for x in raw_list]

    #Compare to produce list of those which are new
    new_ids = []
    for id in wks_unq_ids:
        if int(id) not in db_unq_ids:
            new_ids.append(id)

    if new_or_all == "new":
        write_to_db(new_ids)
    elif new_or_all == "all":
        write_to_db(wks_unq_ids)
    else:
        raise Exception("Error: Input either 'new' or 'all'")

    #Closing the cursor
    mdb_cur.close()
    #Committing changes
    media_db_conn.commit()
    #Closing Connection
    media_db_conn.close()

#||||||||||||||||||||||||||||||||||
#|||MIRROR DB ONTO GOOGLE SHEETS|||
#||||||||||||||||||||||||||||||||||

#Used in function calls in below function
def mirror_db_gsheet(target_wks, headers_list,db_cursor):

    #Unpack cursor object
    db_list = db_cursor.fetchall()

    #Make sure it isn't a tuple by repacking it into a list of lists
    db_is_list_type = []
    for potential_tuple in db_list:
        sub_list = []
        for x in potential_tuple:
            sub_list.append(x)
        db_is_list_type.append(sub_list)

    #Delete Everything
    target_wks.clear()

    #Target for combine with headers, start with headers
    combo_list = []
    combo_list.append(headers_list)

    #Append all the db data
    if target_wks == wks_mov_unq: # Step for dates in mov unique vals
        for my_list in db_is_list_type:
            combo_list.append(my_list)
    elif target_wks == wks_pm_vals: #Step for dates in per movie vals
        for my_list in db_is_list_type:
            new_list1 = my_list
            new_list1[0] = str(new_list1[0].strftime("%Y-%m-%d %H:%M:%S"))
            new_list1[3] = str(new_list1[3].strftime("%Y-%m-%d %H:%M:%S"))
            combo_list.append(new_list1)
    elif target_wks == wks_mov_watch_vals: #Step to add additional gsheets only row, which acts as a psuedo ranking field in tableau
        for my_list in db_is_list_type:
            new_list1 = my_list
            new_list1[0] = str(new_list1[0].strftime("%Y-%m-%d %H:%M:%S"))
            new_list1.insert(0,"=ROW()-1")
            combo_list.append(new_list1)
    else: #Step for everything else which starts with a datetime object in index 0
        for my_list in db_is_list_type:
            new_list = my_list
            #Change datetime object to string and store in var
            date_time1 = str(new_list[0].strftime("%Y-%m-%d %H:%M:%S"))
            #Remove datetime object
            new_list.pop(0)
            #Insert compatible string of datetime in same position
            new_list.insert(0,date_time1)
            #Append each list to the new combo list
            combo_list.append(new_list)
    #Write new data to gsheet - uses diff input option for movie watch values so the above sheet formula will work
    if target_wks == wks_mov_watch_vals:
        target_wks.append_rows(combo_list,value_input_option='USER_ENTERED')
    else:
        target_wks.append_rows(combo_list)

#Calling the above function, committing db to g-sheets tables
def commit_db_gsheet_mirror():
    #Creating connection object
    media_db_conn = psycopg2.connect(dbname = DB_NAME,user = DB_USER, password = DB_PASS, host = DB_HOST)

    #Creating cursors and saving in list vars

    #Box Office
    gbox_cur = media_db_conn.cursor()
    gbox_cur.execute("SELECT entry_datetime,movie_id,type,string,numbers_extract FROM box_office;")
    media_db_conn.commit()
    box_db_list = gbox_cur
    mirror_db_gsheet(wks_box_off,["entry_datetime","movie_id","type","string","numbers_extract"],box_db_list)
    gbox_cur.close()

    #Companies
    comp_cur = media_db_conn.cursor()
    comp_cur.execute("SELECT entry_datetime,movie_id,type,company_id,company_name FROM movie_companies;")
    media_db_conn.commit()
    comp_db_list = comp_cur
    mirror_db_gsheet(wks_mov_comp,["entry_datetime","movie_id","type","company_id","company_name"],comp_db_list)
    comp_cur.close()

    #Movie List Values
    mlist_cur = media_db_conn.cursor()
    mlist_cur.execute("SELECT entry_datetime,movie_id,type,value_string1,value_string2 FROM movie_list_values;")
    media_db_conn.commit()
    mlist_db_list = mlist_cur
    mirror_db_gsheet(wks_mov_list_val,["entry_datetime","movie_id","type","value_string1","value_string2"],mlist_db_list)
    mlist_cur.close()

    #People
    ppl_cur = media_db_conn.cursor()
    ppl_cur.execute("SELECT entry_datetime,movie_id,type,person_id,person_name FROM movie_people;")
    media_db_conn.commit()
    ppl_db_list = ppl_cur
    mirror_db_gsheet(wks_mov_ppl,["entry_datetime","movie_id","type","person_id","person_name"],ppl_db_list)
    ppl_cur.close()

    #per movie values
    pmv_cur = media_db_conn.cursor()
    pmv_cur.execute("SELECT entry_datetime,movie_id,localized_title,original_air_date,original_air_country,rating,votes,plot_outline,title,year,kind,cover_url,full_size_cover_url,cover_hex_dominant,cover_hex_palette1,cover_hex_palette2,cover_hex_palette3,cover_hex_palette4,cover_hex_palette5 FROM per_movie_values;")
    media_db_conn.commit()
    pmv_db_list = pmv_cur
    mirror_db_gsheet(wks_pm_vals,["entry_datetime","movie_id","localized_title","original_air_date","original_air_country","rating","votes","plot_outline","title","year","kind","cover_url","full_size_cover_url","cover_hex_dominant","cover_hex_palette1","cover_hex_palette2","cover_hex_palette3","cover_hex_palette4","cover_hex_palette5"],pmv_db_list)
    pmv_cur.close()

    #unique movie values
    mov_unq_cur = media_db_conn.cursor()
    mov_unq_cur.execute("SELECT given_media_name,url,movie_id,data_source FROM movies_unique;")
    media_db_conn.commit()
    mov_unq_db_list = mov_unq_cur
    mirror_db_gsheet(wks_mov_unq,["given_media_name","url","movie_id","data_source"],mov_unq_db_list)
    mov_unq_cur.close()

    #per movie WATCH values
    mov_watch_unq_cur = media_db_conn.cursor()
    #Includes ORDER BY clause to determine filter order in Tableau workbook
    mov_watch_unq_cur.execute("SELECT datetime_finished,given_media_name,platform,short_comment,movie_id FROM movie_per_watch_values ORDER BY datetime_finished;")
    media_db_conn.commit()
    mov_watch_vals_list = mov_watch_unq_cur
    #includes additional column name for column which only appears in gsheets
    mirror_db_gsheet(wks_mov_watch_vals,["watch_rank","datetime_finished","given_media_name","platform","short_comment","movie_id"],mov_watch_vals_list)
    mov_watch_unq_cur.close()

    # Committing any remaining changes
    media_db_conn.commit()
    # Closing Connection
    media_db_conn.close()

#Function dedicated to mirroring the write_log into google sheets just to help with log flow below
#Date of log is added by database as default value
def mirror_mov_write_log():
    #Creating connection object
    media_db_conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    #Creating cursor object
    logs_cur = media_db_conn.cursor()
    #Writing this mirror attempt to the log
    write_log("google sheets", "mirrored mov log table to log table")
    #Creating list to be appended into, starting with table headers
    data_list = [["datetime_log","log_name","action_name"]]
    logs_cur.execute("SELECT datetime_log,log_name,action_name FROM mov_write_log;")
    #Committing the select statement into the cursor
    media_db_conn.commit()
    #Assigning the cursor to a var
    log_listv1 = logs_cur
    #Repacking log list to remove any tuples
    log_listv2 = []
    for potential_tuple in log_listv1:
        sub_list = []
        for x in potential_tuple:
            sub_list.append(x)
        log_listv2.append(sub_list)
    #Changing datetime type to string for JSON compatibility
    for my_list in log_listv2:
        my_list[0] = str(my_list[0].strftime("%Y-%m-%d %H:%M:%S"))
    #Adding logs to data list
    for row in log_listv2:
        data_list.append(row)
    #Clearing the gsheet
    wks_mov_write_log.clear()
    #Adding new data to the gsheet
    wks_mov_write_log.append_rows(data_list)
    # Committing any remaining changes
    media_db_conn.commit()
    # Closing Connection
    media_db_conn.close()

#|||||||||||||||||||||||||||
#|||RUNNING THE FUNCTIONS|||
#|||||||||||||||||||||||||||

#Summary function to run features of this code. Will be called in other scripts.
#Writes logs to log table
def run_it(option):
    #try:
    if option == "updatenew" and len(new_ids_list) > 0: #if update new option selected and there are new ids
        write_log("newness check success", "new id list length greater than zero")
        update_db_method("new") #only update new ids
        write_log("database update", "new ids only")
    elif option == "updatenew" and len(new_ids_list) == 0:
        write_log("newness check failure", "new id list length equal to zero - no update made")
    elif option == "updateall": #re-update everything
        write_log("update all request", "will attempt to grab info for all movie ids")
        update_db_method("all") #get data for all ids (can take time) and write to db
        write_log("update all success", "updated all new movie data into database")
    else:
        print("Invalid option, enter either 'updatenew' or 'updateall'")

    commit_db_gsheet_mirror() #Mirror database to google sheets
    write_log("google sheets", "database mirrored onto tables")
    mirror_mov_write_log() #Mirror log file to google sheets
    #except:
        #write_log("error", "some error was encountered when running the script, did not complete execution")