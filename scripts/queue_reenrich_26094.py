from harvester.post_processing.couchdb_runner import CouchDBJobEnqueue
from harvester.post_processing.enrich_existing_couch_doc import main as reenrich

results = CouchDBJobEnqueue().queue_collection("26094",
                                            14400,
                                            reenrich, 
          '/lapl-marc-id,/dpla_mapper?mapper_type=lapl_marc'
          )

print results
