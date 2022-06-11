## Supporting Scripts

### color_palette_vals_from_url.py
Provides a function to generate a pallette of the 5 most dominant colors in a provided image URL. This is used in the script to generate hex color codes for the movie details database. The original intention was to have a dynamic dashboard theme based on the selected movie, but unfortunately this was possible. However, future changes might be able to make use of the data so the feature was left in.

### move_data_flow_updateall.py
Runs the core function from the main script to upsert data for all movies.

### movie_data_flow_updatenew.py
Runs the core function from the main script to pull the data only for new movies I've watched that don't exist in the database yet. Includes a 60 second timer to allow the database time to boot up when I start my computer.

### run_mov_dataflow_updatenew.bat
A basic bat file which runs the data flow when I start my computer up.
