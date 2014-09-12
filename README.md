twitter
=======

This repository contains scripts relating to a Twitter tweet analysis project. Most of the functionality is not intended to be a general purpose, user-facing API, so it will be more specific rather than more general---scripts will assume a particular schema for databases, etc. That said, many of the ideas/implementations are probably of more general interest for me in other projects, and possibly, for others as well.

The project uses Python as the main tool. Tweets are stored in raw text files
and migrated to a mongodb database. County information is taken from the 2010
census. shapely, fiona, d3, topojson, and the Google Maps API are used for 
some simple visualization of the data.

Folders
-------

-   *data* : Holds scripts for collecting tweet data from Twitter.
-   *info* : Holds links and other pieces of information relevant to project.
-   *scripts* : Holds scripts that use the data and `twitterproj` library.
-   *twitterproj* : The Python "library" that should be added to your
    site-packages. Various scripts will expect the following code to
    succeed: `import twitterproj`.

The `requirements.txt` file lists packages that scripts in this project
will need. Install them all at once:

    pip install -r requirements.txt
