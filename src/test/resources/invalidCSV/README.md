Invalid instructions.csv file
=============================

Run the following command: `./run.sh path/to/invalidCSV path/to/output invalid-datamanager-id` where:
    * `path/to/invalidCSV` is the path to this dataset
    * `path/to/output` is the path to the output folder where the resulting deposit is supposed to end up 

This dataset contains an `instructions.csv` file that will be marked as invalid on the following values:

* DDM_CREATED
    * '_invalid-date_' does not represent a date
* DDM_ACCESSRIGHT
    * more than one value is defined
* DDM_AVAILABLE
    * '_invalid-date_' does not represent a date
* DC_TYPE
    * not a valid type given
* DC_LANGUAGE
    * not a valid (ISO639.2) language
    * '_encoding=UTF-8_' does not represent a valid language
* DCT_DATE
    * '_Text with Qualifier_' does not represent a date
    * '_30-07-1992_' does not represent a date
* DCT_DATE_QUALIFIER
    * missing DCT_DATE value to go with this qualifier
* SF_USER
    * no value found
* FILE_PATH
    * 'path/to/audiofile/that/does/not/exist.mp3' does not exist
* DEPOSITOR_ID
    * multiple distinct depositorIDs: {user001, invalid-user}
* DC_IDENTIFIER
    * no value found

**Note:** there are more invalid values in `instructions.csv`, but these are only found when this
first badge of errors is resolved.

* DDM_AVAILABLE
    * more than one value is defined
* DDM_AUDIENCE/DDM_ACCESSRIGHT
    * when DDM_ACCESSRIGHTS is GROUP_ACCESS, DDM_AUDIENCE should be D37000 (Archaeologie), but it is: D30000
* SF_DOMAIN
    * only characters `[a-zA-Z0-9-_]` are allowed in this property
* SF_COLLECTION
    * only characters `[a-zA-Z0-9-_]` are allowed in this property
* SF_USER/AV_FILE
    * the column AV_FILE contains values, but the column(s) [SF_USER] do not
