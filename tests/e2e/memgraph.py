import mgclient

MEMGRAPH_DESTINATION_HOST = '127.0.0.1'
MEMGRAPH_DESTINATION_PORT = 7687


def clean_memgraph(host, port):
    conn = mgclient.connect(host=host, port=port)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("MATCH (n) DETACH DELETE n;")
    cursor.fetchall()

    cursor.execute("MATCH (n) RETURN COUNT(n);")
    row = cursor.fetchone()
    assert len(row) == 1 and row[0] == 0, "Failed to clear Memgraph"
    assert not cursor.fetchone()

    # remove all the constraints
    cursor.execute("SHOW CONSTRAINT INFO")
    rows = cursor.fetchall()

    for constraint_type, label, properties in rows:
        if constraint_type == 'exists':
            cursor.execute(
                f'DROP CONSTRAINT ON (n:{label}) ASSERT exists (n.{properties})')
            cursor.fetchall()
        else:
            assert constraint_type == 'unique', "Invalid constraint type"
            property_list = ', '.join(f'n.{p}' for p in properties)
            cursor.execute(
                f'DROP CONSTRAINT ON (n:{label}) ASSERT {property_list} IS UNIQUE')
            cursor.fetchall()

    cursor.execute("SHOW CONSTRAINT INFO")
    rows = cursor.fetchall()
    assert len(rows) == 0, "Failed to clean constraints"


def validate_imdb(labels_with_prefix):
    label_prefix = 'imdb_' if labels_with_prefix else ''
    conn = mgclient.connect(
        host=MEMGRAPH_DESTINATION_HOST,
        port=MEMGRAPH_DESTINATION_PORT)
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute(
        f'MATCH (a:{label_prefix}actors {{name: "Christian Bale"}})-[:{label_prefix}movie_roles]->(m:{label_prefix}movies) '
        'WHERE m.rating >= 8 '
        'RETURN m.title, m.rating '
        'ORDER BY m.rating DESC')
    rows = cursor.fetchall()
    expected_rows = [
        ('The Dark Knight',
         9.0),
        ('The Prestige',
         8.5),
        ('The Dark Knight Rises',
         8.4),
        ('Batman Begins',
         8.2),
        ('Ford v Ferrari',
         8.1)]
    assert rows == expected_rows, f"Invalid results returned. Actual result: {rows}, expected result: {expected_rows}"

    cursor.execute(
        f'MATCH path = (:{label_prefix}actors {{name: "George Clooney"}})-[* bfs]-(:{label_prefix}actors {{name: "Kevin Bacon"}}) '
        'RETURN size(path) / 2')
    row = cursor.fetchone()
    assert len(row) == 1 and row[0] == 2, "Got invalid path from Memgraph"
    assert not cursor.fetchone()

    cursor.execute(
        'SHOW CONSTRAINT INFO')
    rows = cursor.fetchall()
    expected_rows = [
        ('exists',
         f'{label_prefix}actors',
         'actor_id'),
        ('exists',
         f'{label_prefix}actors',
         'name'),
        ('exists',
         f'{label_prefix}movies',
         'movie_id'),
        ('exists',
         f'{label_prefix}movies',
         'title'),
        ('exists',
         f'{label_prefix}tvseries',
         'series_id'),
        ('exists',
         f'{label_prefix}tvseries',
         'title'),
        ('exists',
         f'{label_prefix}tvepisodes',
         'series_id'),
        ('exists',
         f'{label_prefix}tvepisodes',
         'episode_id'),
        ('unique',
         f'{label_prefix}actors',
         ['actor_id']),
        ('unique',
         f'{label_prefix}movies',
         ['movie_id']),
        ('unique',
         f'{label_prefix}tvseries',
         ['series_id']),
        ('unique',
         f'{label_prefix}tvepisodes',
         ['episode_id'])]
    assert len(rows) == len(expected_rows) and all(elem in rows for elem in expected_rows
                                                   ), f"Invalid constraints returned. Constraints: {rows}, expected constaints: {expected_rows}"
