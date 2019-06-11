def couchdb_pager(db, view_name='_all_docs',
                  startkey=None, startkey_docid=None,
                  endkey=None, endkey_docid=None,
                  key=None,
                  bulk=200000, **extra_options):
    # Request one extra row to resume the listing there later.
    options = {'limit': bulk + 1}
    print("EXTRA: {}".format(extra_options))
    if extra_options:
        options.update(extra_options)
    if startkey:
        #works with underscore, but should without?
        ###options['startkey'] = startkey #not working
        options['start_key'] = startkey
        if startkey_docid:
            options['startkey_docid'] = startkey_docid
    if endkey:
        ###options['endkey'] = endkey
        options['end_key'] = endkey
        if endkey_docid:
            options['endkey_docid'] = endkey_docid
    if key:
        options['key'] = key
    done = False
    while not done:
        print("OPTS:{}".format(options))
        view = db.view(view_name, **options)
        print("VIEW:{} LEN:{}".format(view, len(view)))
        rows = []
        # If we got a short result (< limit + 1), we know we are done.
        if len(view) <= bulk:
            done = True
            rows = view.rows
        else:
            # Otherwise, continue at the new start position.
            rows = view.rows[:-1]
            last = view.rows[-1]
            ###options['start_key'] = last.key
            options['startkey_docid'] = last.id

        for row in rows:
            yield row
