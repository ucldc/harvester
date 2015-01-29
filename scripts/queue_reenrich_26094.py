from harvester.post_processing.couchdb_runner import CouchDBJobEnqueue
from harvester.post_processing.enrich_existing_couch_doc import main as reenrich

enrichments = '''/lapl-marc-id,
          /dpla_mapper?mapper_type=lapl_marc,
          /strip_html,
          /set_context,
          /cleanup_value,
          /move_date_values?prop=sourceResource%2Fsubject,
          /move_date_values?prop=sourceResource%2Fspatial,
          /shred?prop=sourceResource%2Fspatial&delim=--,
          /capitalize_value?exclude=sourceResource%2Frelation,
          /enrich-subject,
          /enrich_earliest_date,
          /enrich_date,
          /enrich-type,
          /enrich-format,
          /enrich_location,
          /copy_prop?prop=sourceResource%2Fpublisher&to_prop=dataProvider,
          /enrich_language,
          /lookup?prop=sourceResource%2Flanguage%2Fname&target=sourceResource%2Flanguage%2Fname&substitution=iso639_3,
          /lookup?prop=sourceResource%2Flanguage%2Fname&target=sourceResource%2Flanguage%2Fiso639_3&substitution=iso639_3&inverse=True,
          /copy_prop?prop=provider%2Fname&to_prop=dataProvider&skip_if_exists=True,
          /set_prop?prop=sourceResource%2FstateLocatedIn&value=California,
          /enrich_location?prop=sourceResource%2FstateLocatedIn,
          /dedupe-sourceresource,
          /validate_mapv3'''

enrichments = enrichments.replace('\n','').replace(' ','')
print enrichments
results = CouchDBJobEnqueue().queue_collection("26094",
                                            30000,
                                            reenrich, 
                                            enrichments
          )

print results
