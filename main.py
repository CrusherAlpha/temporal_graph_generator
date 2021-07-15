import yaml
import os
import csv
from datetime import datetime
import random
import time

# vertex [0, vertex_counter)
# edge [vertex_counter, entity_counter)

entity_id_counter = 0
vertex_counter = 0
edges = {}
has_vertex = {}


def parse_config(file):
    with open(file, "r") as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)
    return cfg['time'], cfg['vertex'], cfg['edge']


def parse_time(given_time):
    start = int(datetime.timestamp(given_time['start']))
    end = int(datetime.timestamp(given_time['end']))
    return start, end, given_time['step']


def parse_entity(entity):
    tp_number = entity['temporal_property_number']
    tp_names = []
    for i in range(1, tp_number + 1):
        tp_names.append('tp' + str(i))
    return entity['total'], tp_number, tp_names, entity['update_proportion'], entity[
        'temporal_property_update_proportion'], entity['temporal_property_type']


def check_edge(s, e):
    if s == e:
        return False
    return (str(s) + '_' + str(e)) not in edges


def update_state(s, e):
    edges[str(s) + '_' + str(e)] = True
    has_vertex[s] = True


def generate_edge():
    outer_threshold = 10_000
    inner_threshold = 10_000
    # generate edge randomly.
    outer_counter = 0
    while True:
        s = random.randint(0, v_number - 1)
        inner_counter = 0
        while True:
            e = random.randint(0, v_number - 1)
            if check_edge(s, e):
                update_state(s, e)
                return s, e
            inner_counter += 1
            if inner_counter > inner_threshold:
                break
        outer_counter += 1
        if outer_counter > outer_threshold:
            break
    # traverse all the vertexes to find an unvisited one.
    for s in range(v_number):
        if s not in has_vertex:
            while True:
                e = random.randint(0, v_number - 1)
                if s != e:
                    update_state(s, e)
                    return s, e
    # traverse all the edges to find a unvisited one.
    for s in range(v_number):
        for e in range(v_number):
            if check_edge(s, e):
                update_state(s, e)
                return s, e
    # can not generate an edge, may be the config is not reasonable.
    print('Can not generate an edge, may be set too much edges than vertexes in config file.')


def generate_topology():
    global entity_id_counter
    # generate vertexes.
    with open('vertex.csv', 'w') as out:
        out_w = csv.writer(out, lineterminator='\n')
        out_w.writerow(['vertex_id'])
        for _ in range(v_number):
            out_w.writerow([entity_id_counter])
            entity_id_counter += 1
    global vertex_counter
    vertex_counter = entity_id_counter
    # generate edges.
    with open('edge.csv', 'w') as out:
        out_w = csv.writer(out, lineterminator='\n')
        out_w.writerow(['edge_id', 'start_vertex_id', 'end_vertex_id'])
        for _ in range(e_number):
            s, e = generate_edge()
            out_w.writerow([entity_id_counter, s, e])
            entity_id_counter += 1
    store_state()


def temporal_data_body(entity_id, cur_time, last, entity_proportion, property_update_proportion, property_type):
    if random.random() > entity_proportion:
        return False, None
    body = [cur_time, entity_id]
    for i, p in enumerate(property_update_proportion):
        if last[entity_id][i] is None or random.random() < p:
            # true means int.
            if property_type[i]:
                body.append(random.randint(0, 1000))
            else:
                # make (0, 1) -> (-1, 1)
                body.append(2 * random.random() - 1)
        else:
            body.append(last[entity_id][i])
    return True, body


def generate_temporal_data():
    with open('vertex_temporal_data.csv', 'w') as out1, open('edge_temporal_data.csv', 'w') as out2:
        w1 = csv.writer(out1, lineterminator='\n')
        w2 = csv.writer(out2, lineterminator='\n')
        vertex_header = ['posix_timestamp', 'vertex_id'] + vp_names
        edge_header = ['posix_timestamp', 'edge_id'] + ep_names
        w1.writerow(vertex_header)
        w2.writerow(edge_header)
        last_v = dict.fromkeys([i for i in range(vertex_counter)], [None for _ in range(vp_number)])
        last_e = dict.fromkeys([i for i in range(vertex_counter, entity_id_counter)], [None for _ in range(ep_number)])
        for cur_time in range(start_time, end_time, step):
            # traverse edges, vertexes and their temporal properties.
            for v in range(vertex_counter):
                ok, body = temporal_data_body(v, cur_time, last_v, v_proportion, vp_update_proportion, vp_type)
                if ok:
                    last_v[v] = body
                    w1.writerow(body)
            for e in range(vertex_counter, entity_id_counter):
                ok, body = temporal_data_body(e, cur_time, last_e, e_proportion, ep_update_proportion, ep_type)
                if ok:
                    last_e[e] = body
                    w2.writerow(body)


def store_state():
    cfg = {'entity_id_counter': entity_id_counter, 'vertex_counter': vertex_counter}
    with open('state.yml', 'w') as f:
        yaml.dump(cfg, f)


def restore_state():
    if not os.path.exists('state.yml'):
        return False
    with open('state.yml', 'r') as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)
        global entity_id_counter
        entity_id_counter = cfg['entity_id_counter']
        global vertex_counter
        vertex_counter = cfg['vertex_counter']
    return True


if __name__ == '__main__':
    random.seed(time.time())
    t, vertex, edge = parse_config('config.yml')
    start_time, end_time, step = parse_time(t)
    v_number, vp_number, vp_names, v_proportion, vp_update_proportion, vp_type = parse_entity(vertex)
    e_number, ep_number, ep_names, e_proportion, ep_update_proportion, ep_type = parse_entity(edge)
    has_topology = restore_state()
    if not has_topology:
        generate_topology()
    generate_temporal_data()
