#
#  [K4toLL] Maintain folder structure in LucidLink volume based on layouts in K4
#
#  K4toLL: 1.0
#
#  Changelog:
#		- 1.0 (12th September 2022):
#			- First version
#
#  Author: Marco Baldassarre <marco.baldassarre@condenast.com>
#

import json
import os
import sys

import pymssql  # on Windows, open a cmd and run: py -m pip install pymssql

# import _scproxy  # workaround for _iconv error on Mac OS X -- see https://github.com/pymssql/pymssql/issues/705

from _configuration import *

# persistence of K4 layouts on local file
dirs_json_path = os.path.join(os.path.dirname(__file__), 'dirs.json')
if os.path.isfile(dirs_json_path):
    with open(dirs_json_path, 'r') as f:
        layouts_last_known_directory = json.load(f)
else:
    layouts_last_known_directory = {}  # empty dict

# loop through all K4 databases configured
for mssql_db in mssql_dbs:
    # connect to DB server
    k4db = pymssql._mssql.connect(server=mssql_db['host'],
                                  user=mssql_username,
                                  password=mssql_password,
                                  database=mssql_db['database'])

    # fetch rows from table
    k4db.execute_query('''
		SELECT
			Publication.name as title,
			Issue.name as issueName,
			Issue.idx as issueNumber,
			publicationDate,
			Section.name as section,
			K4ObjectVariant.id as layoutId,
			K4Object.Name as layoutName
						
		FROM 
			K4Object 
			INNER JOIN K4ObjectVariant ON K4Object.id = K4ObjectVariant.K4ObjectID
			INNER JOIN Issue ON K4ObjectVariant.issueID = Issue.id
							AND K4ObjectVariant.publicationID = Issue.PublicationID
			INNER JOIN Publication ON Issue.PublicationID = Publication.id
			INNER JOIN Section ON K4ObjectVariant.sectionID = Section.id
		
		WHERE 
			K4Object.K4ObjectType = 0 AND
			(Issue.type=0 OR Issue.type=4) AND
			Publication.active=1 AND
			publicationDate IS NOT NULL AND 
			publicationDate > CAST(DATEDIFF(s, '1970-01-01 00:00:00', ''' + minimum_publication_date + ''') AS BIGINT)*1000''')
    # K4Object.K4ObjectType = 0 : Layouts only

    for k4layout in k4db:
        market_brand_level = publication_name_mapping.get(k4layout['title'])
        if market_brand_level is None:
            print(k4layout['title'], 'not in the mapping dictionary, skipping...', file=sys.stderr)
            continue

        year = k4layout['issueName'][0:4]
        layout_id = str(k4layout['layoutId'])

        layout_directory = os.path.join(market_brand_level, year, k4layout['issueName'], k4layout['layoutName'])
        layout_last_known_directory = layouts_last_known_directory.get(layout_id)

        if layout_last_known_directory is None:
            # new layout, let's create its folders

            # create the same structure within Editorial and Repro
            for root_level in ['Editorial', 'Repro']:
                os.makedirs(os.path.join(lucidlink_root, root_level, layout_directory), exist_ok=True)

        elif layout_last_known_directory != layout_directory:
            # layout directory needs moving!

            # create the same structure within Editorial and Repro
            for root_level in ['Editorial', 'Repro']:
                try:
                    os.rename(
                        src=os.path.join(lucidlink_root, root_level, layout_last_known_directory),
                        dst=os.path.join(lucidlink_root, root_level, layout_directory)
                    )
                except FileExistsError:
                    # If dst exists, do nothing. It may already have been renamed manually.
                    pass
        else:
            # do nothing, as nothing has changed
            pass

        # maintain the local dictionary updated
        layouts_last_known_directory[layout_id] = layout_directory

# persistence of K4 layouts on local file
with open(dirs_json_path, 'w') as f:
    json.dump(layouts_last_known_directory, f)
