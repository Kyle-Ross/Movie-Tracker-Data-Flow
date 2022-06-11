#||||||||||||||||||||||||||||||||||||||||||||||||||||||||
#|||FUNCTION TO GET PALETTE COLOR VALUES FROM IMG URLS|||
#||||||||||||||||||||||||||||||||||||||||||||||||||||||||

#https://github.com/fengsp/color-thief-py

#ColorThief module to get RGB values from the image object
from colorthief import ColorThief

#Imported to get an appropriate image object from a URL
from imageio import v3 as iio
import io

#Convert RGB typles to hex values
def convert_to_hex(my_tuple):
    hex_string = '#%02x%02x%02x' % my_tuple
    return hex_string.upper()

#Convert a img URL to a img bytes obj I can use in other functions
def img_obj_create(my_url):
    #Load the image
    img = iio.imread(my_url)
    #Write as JPG, save in output variable
    #Make it a Bytes object so it works with the next function
    output = io.BytesIO()
    iio.imwrite(output, img, plugin="pillow", format="JPEG")
    return output
    #Note - I don't really understand why this works and the other stuff didn't, just got it from googling

#function to generate different image color detail outputs
def from_img_file(img_file, opt = "dominant", qual_val = 6, col_cnt = 5):
    color_thief = ColorThief(img_file)
    if opt == "dominant":
        dominant_color = color_thief.get_color(quality=1) #get the dominant color
        return dominant_color
    elif opt == "palette":
        palette = color_thief.get_palette(quality=qual_val, color_count=col_cnt) #get a palette of x size and quality
        return palette
    else:
        raise Exception("opt value must be 'dominant' or 'palette'")

#Combines the other functions and provides an option to have hex or rgb values
def img_color_from_url(my_url, rgbhex = 'hex', my_opt = "dominant", my_qual_val = 6, my_col_cnt = 5):
    img_obj = img_obj_create(my_url)
    if rgbhex == 'rgb':
        return from_img_file(img_obj, opt=my_opt, qual_val=my_qual_val, col_cnt=my_col_cnt)
    elif rgbhex == 'hex':
        tuples_list = from_img_file(img_obj, opt=my_opt, qual_val=my_qual_val, col_cnt=my_col_cnt)
        if my_opt == "dominant":
            return convert_to_hex(tuples_list)
        elif my_opt == "palette":
            hex_list = []
            for x in tuples_list:
                as_hex = convert_to_hex(x)
                hex_list.append(as_hex)
            return hex_list
    else:
        raise Exception("rbghex value must be 'rgb' or 'hex'")
