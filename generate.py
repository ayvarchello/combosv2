import xml.etree.ElementTree as ET
import os
import stat


tree = ET.parse("parameters.xml")
simulation_time = tree.find('simulation_time').text
server_side = tree.find('server_side')
client_side = tree.find('client_side')

files = ['platform_my.xml', 'deployment_my.xml']
nprojects = int(server_side.find('n_projects').text)
nclients = [0] * nprojects
nservers = [None] * nprojects
napps = [None] * nprojects
total_clients = 0
total_scheduling_servers = 0
total_data_servers = 0

platform = ET.Element('platform', {'version': '3'})
AS = ET.SubElement(platform, 'AS', {'id': 'AS0', 'routing': 'Full'})
sub_AS = ET.SubElement(AS, 'AS', {'id': 'AS1', 'routing': 'None'})
ET.SubElement(sub_AS, 'host', {'id': 'r0', 'power': '1'})

deployment = ET.Element('platform', {'version': '3'})
ET.SubElement(deployment, 'process', {'host': 'r0', 'function': 'print_results'})

proj_args = ['snumber', 'name', 'long_precentage', 'ifgl_percentage', 'ifcd_percentage',
             'disk_bw', 'ndata_servers', 'nplatforms']

app_args = ['output_file_size', 'task_fpops',  'min_quorum', 'target_nresults', 
            'max_error_results', 'max_total_results',
            'max_success_results', 'delay_bound', 'success_percentage',
            'canonical_percentage', 'input_file_size', 'replication', 'ntotal_tasks', 
            'name', 'start_time']

client_args = ['n_clients', 'connection_interval', 'scheduling_interval', 'max_speed', 
               'min_speed', 'pv_distri', 'pa_param', 'pb_param', 'av_distri', 'aa_param',
               'ab_param', 'nv_distri', 'na_param', 'nb_param']

ngroups = int(client_side.find('n_groups').text)

data_serv_num = 0
for idx, project in enumerate(server_side.findall('sproject')):
    num = int(project.find('snumber').text)
    proj_apps = int(project.find('napplications').text)
    num_ss = int(project.find('nscheduling_servers').text)
    total_scheduling_servers += num_ss
    nservers[num] = num_ss
    napps[num] = proj_apps
    

    init = ET.SubElement(deployment, 'process',
                         {'host': 'b' + str(num), 'function': 'init_database'})
    for arg in proj_args:
        ET.SubElement(init, 'argument', {'value': project.find(arg).text})
    for app in project.findall('application'):
        for arg in app_args:
            ET.SubElement(init, 'argument', {'value': app.find(arg).text})

        platform_mask = 0
        for b_platform in app.findall('platform'):
            platform_mask += 2 ** int(b_platform.text)
    
        ET.SubElement(init, 'argument', {'value': str(platform_mask)})

    for proc in ('work_generator', 'validator', 'assimilator'):
        for app in range(proj_apps):
            elem = ET.SubElement(deployment, 'process',
                                 {'host': 'b' + str(num), 'function': proc})
            ET.SubElement(elem, 'argument', {'value': str(num)})
            ET.SubElement(elem, 'argument', {'value': str(app)})
            
    for ssnum in range(num_ss):
        for proc in ('scheduling_server_requests', 'scheduling_server_dispatcher'):
            elem = ET.SubElement(deployment, 'process',
                                 {'host': 's' + str(num + 1) + str(ssnum), 'function': proc})
            ET.SubElement(elem, 'argument', {'value': str(num)})
            ET.SubElement(elem, 'argument', {'value': str(ssnum)})
    
    num_ds = int(project.find('ndata_servers').text)
    total_data_servers += num_ds
    for dnum in range(num_ds):
        elem = ET.SubElement(deployment, 'process',
                             {'host': 'd' + str(num + 1) + str(ssnum), 
                              'function': 'data_server_requests'})
        ET.SubElement(elem, 'argument', {'value': str(data_serv_num)})

        elem = ET.SubElement(deployment, 'process',
                             {'host': 'd' + str(num + 1) + str(ssnum), 
                              'function': 'data_server_dispatcher'})
        ET.SubElement(elem, 'argument', {'value': str(data_serv_num)})
        ET.SubElement(elem, 'argument', {'value': str(num)})

        data_serv_num += 1
        
    power = project.find('server_pw').text
    sub_AS = ET.SubElement(AS, 'AS', {'id': 'BE{}'.format(idx+1), 'routing': 'None'})
    ET.SubElement(sub_AS, 'host', {'id': 'b{}'.format(idx), 
                                   'power': power})
    
    ET.SubElement(AS, 'cluster', {
        'id': 'cluster_{}'.format(ngroups + 1 + idx * 2),
        'prefix': 's{}'.format(idx + 1),
        'suffix': '',
        'radical': '0-{}'.format(num_ss),
        'power': power,
        'bw': '100Gbps',
        'lat': '5ms',
        'router_id': 'router_cluster{}'.format(ngroups + 1 + idx * 2),
    })
    ET.SubElement(AS, 'cluster', {
        'id': 'cluster_{}'.format(ngroups + 2 + idx * 2),
        'prefix': 'd{}'.format(idx + 1),
        'suffix': '',
        'radical': '0-{}'.format(num_ds),
        'power': '1Gf',
        'bw': '100Gbps',
        'lat': '5ms',
        'router_id': 'router_cluster{}'.format(ngroups + 2 + idx * 2),
    })


link_num = 0
route_num = 0
routes = []
for idx, group in enumerate(client_side.findall('group')):
    group_clients = int(group.find('n_clients').text)
    total_clients += group_clients
    
    trace_path = group.find('traces_file').text
    trace_file = None
    if os.path.isfile(trace_path):
        trace_file = open(trace_path, 'r')
        
    client0 = ET.SubElement(deployment, 'process', 
                            {'host': 'c{}0'.format(idx + 1), 'function': 'client'})
    ET.SubElement(client0, 'argument', {'value': str(idx)})
    ET.SubElement(client0, 'argument', {'value': '0'})
    for arg in client_args:
        ET.SubElement(client0, 'argument', {'value': group.find(arg).text})
    if trace_file:
        ET.SubElement(client0, 'argument', {'value': trace_file.readline().strip()})
    ET.SubElement(client0, 'argument', {'value': group.find('att_projs').text})
    
    for gproject in group.findall('gproject'):
        pnum = int(gproject.find('pnumber').text)
        nclients[pnum] += group_clients   
        ET.SubElement(client0, 'argument', {'value': 'Project' + str(pnum + 1)})
        ET.SubElement(client0, 'argument', {'value': str(pnum)})
        ET.SubElement(client0, 'argument', {'value': gproject.find('priority').text})
      
    client_num = 1
    for b_platform in group.findall('platform'):
        num = int(b_platform.find('n_clients').text)
        if b_platform.find('number').text == '0':
            num -= 1

        for i in range(num):
            client = ET.SubElement(deployment, 'process', 
                                   {'host': 'c{}{}'.format(idx + 1, client_num), 'function': 'client'})
            client_num += 1
            ET.SubElement(client, 'argument', {'value': str(idx)})
            ET.SubElement(client, 'argument', {'value': b_platform.find('number').text})
            if trace_file:
                ET.SubElement(client, 'argument', {'value': trace_file.readline().strip()})

    ET.SubElement(AS, 'cluster', {
        'id': 'cluster_{}'.format(idx + 1),
        'prefix': 'c{}'.format(idx + 1),
        'suffix': '',
        'radical': '0-{}'.format(group_clients),
        'power': '1Gf',
        'bw': group.find('gbw').text,
        'lat': group.find('glatency').text,
        'router_id': 'router_cluster{}'.format(idx + 1),
    })
        
    for gproject in group.findall('gproject'):
        ET.SubElement(AS, 'link', {
            'id': 'l{}'.format(link_num),
            'bandwidth': gproject.find('lsbw').text,
            'latency': gproject.find('lslatency').text,
        })
        link_num += 1
        ET.SubElement(AS, 'link', {
            'id': 'l{}'.format(link_num),
            'bandwidth': gproject.find('ldbw').text,
            'latency': gproject.find('ldlatency').text,
        })
        link_num += 1
        
        dst = ngroups + 1 + 2 * int(gproject.find('pnumber').text)
        routes.append(({
            'src': 'cluster_{}'.format(idx + 1),
            'dst': 'cluster_{}'.format(dst),
            'gw_src': 'router_cluster{}'.format(idx + 1),
            'gw_dst': 'router_cluster{}'.format(dst),
        }, {'id': 'l{}'.format(route_num)}))
        
        route_num += 1
        
        dst += 1
        routes.append(({
            'src': 'cluster_{}'.format(idx + 1),
            'dst': 'cluster_{}'.format(dst),
            'gw_src': 'router_cluster{}'.format(idx + 1),
            'gw_dst': 'router_cluster{}'.format(dst),
        }, {'id': 'l{}'.format(route_num)}))
        route_num += 1
        
    if trace_file:
        trace_file.close()

for i in range(int(server_side.find('n_projects').text)):
    ET.SubElement(AS, 'link', {
        'id': 'l{}'.format(link_num),
        'bandwidth': '10Gbps',
        'latency': '0',
    })
    link_num += 1
    
    src = ngroups + 1 + i * 2
    routes.append(({
        'src': 'cluster_{}'.format(src),
        'dst': 'cluster_{}'.format(src + 1),
        'gw_src': 'router_cluster{}'.format(src),
        'gw_dst': 'router_cluster{}'.format(src + 1),
    }, {'id': 'l{}'.format(route_num)}))
    route_num += 1
    
for route_el in routes:
    route = ET.SubElement(AS, 'ASroute', route_el[0])
    ET.SubElement(route, 'link_ctn', route_el[1])

with open('Files/platform_my.xml', 'wb') as f:
    f.write(b"<?xml version='1.0'?>\n")
    f.write(b'<!DOCTYPE platform SYSTEM "http://simgrid.gforge.inria.fr/simgrid.dtd">\n')
    f.write(ET.tostring(platform, encoding='utf-8', method='xml'))
    
with open('Files/deployment_my.xml', 'wb') as f:
    f.write(b"<?xml version='1.0'?>\n")
    f.write(b'<!DOCTYPE platform SYSTEM "http://simgrid.gforge.inria.fr/simgrid.dtd">\n')
    f.write(ET.tostring(deployment, encoding='utf-8', method='xml'))

execute_str = '''#!/bin/bash
: ${{1?"Usage: $0 ALGORITHM"}}
cd Files
./boinc_simulator {}
cd ..
'''

with open('execute', 'w') as f:
    constants = [nprojects, total_scheduling_servers, total_data_servers, simulation_time, ngroups]
    args = ' '.join(map(str, files + constants + nclients + nservers + napps + [total_clients, '$1', '--cfg=contexts/stack_size:16', '--log=root.threshold:error']))
    f.write(execute_str.format(args))

st = os.stat('execute')
os.chmod('execute', st.st_mode | stat.S_IEXEC)
