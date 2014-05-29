"""
Script to run the ingest process.


Usage:
    $ python run_ingest.py user-email url_collection_api
"""
import sys
import os
from email.mime.text import MIMEText
from dplaingestion.scripts import enrich_records
from dplaingestion.scripts import save_records
from dplaingestion.scripts import remove_deleted_records
from dplaingestion.scripts import dashboard_cleanup
from dplaingestion.scripts import check_ingestion_counts
import logbook
import harvester

EMAIL_RETURN_ADDRESS = os.environ.get('RETURN_EMAIL', 'example@example.com')

def create_mimetext_msg(mail_from, mail_to, subject, message):
    msg = MIMEText(message)
    msg['Subject'] = str(subject)
    msg['From'] = mail_from
    msg['To'] = mail_to
    return msg


def def_args():
    import argparse
    parser = argparse.ArgumentParser(description='Harvest a collection')
    parser.add_argument('user_email', type=str, help='user email')
    parser.add_argument('url_api_collection', type=str,
            help='URL for the collection Django tastypie api resource')
    return parser

def main(argv):
    parser = def_args()
    args = parser.parse_args(argv[1:])
    if not args.user_email or not args.url_api_collection:
        parser.print_help()
        sys.exit(27)
    print "EMAIL", args.user_email, " URI: ", args.url_api_collection
    mail_handler = logbook.MailHandler(EMAIL_RETURN_ADDRESS, args.user_email, level=logbook.ERROR) 
    try:
        collection = harvester.Collection(args.url_api_collection)
    except Exception, e:
        mimetext = create_mimetext_msg(EMAIL_RETURN_ADDRESS, args.user_email, 'Collection init failed for ' + args.url_api_collection, ' '.join(("Exception in Collection", args.url_api_collection, " init", str(e))))
        mail_handler.deliver(mimetext, args.user_email)
        raise e
    log_handler = logbook.FileHandler(harvester.get_log_file_path(collection.slug))
    log_handler = logbook.StderrHandler(level='DEBUG')

    ingest_doc_id, num_recs, dir_save = harvester.main(
                        args.user_email,
                        args.url_api_collection,
                        log_handler=log_handler,
                        mail_handler=mail_handler
            )

    print "INGEST DOC ID:", ingest_doc_id
    print 'HARVESTED ', num_recs, ' RECORDS'
    print 'IN DIR:', dir_save
    resp = enrich_records.main([None, ingest_doc_id])
    if not resp == 0:
        print "Error enriching records"
        sys.exit(1)

    print "ENRICHED RECS"
    resp = save_records.main([None, ingest_doc_id])
    if not resp == 0:
        print "Error saving records ", str(resp)
        sys.exit(1)

    print "SAVED RECS"
    resp = remove_deleted_records.main([None, ingest_doc_id]) 
    if not resp == 0:
        print "Error deleting records"
        sys.exit(1)

    resp = check_ingestion_counts.main([None, ingest_doc_id])
    if not resp == 0:
        print "Error checking counts"
        sys.exit(1)

    resp = dashboard_cleanup.main([None, ingest_doc_id])
    if not resp == 0:
        print "Error cleaning up dashboard"
        sys.exit(1)

    print "Ingestion complete!"

if __name__ == '__main__':
    main(sys.argv)
