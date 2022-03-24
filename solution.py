
def raise_exception():
	raise("this is exception for this node")

graph = {
  'root': ['a'], 
  'a': ['b', 'e'], 
  'b': ['c', 'd'], 
  'd': ['e'], 
  'c': [], 
  'e': [] 
}


processes = { 
  "root": lambda: print("root"), 
  "a":    lambda: print("A"), 
  "b":    lambda: print("B"), 
  "c":    lambda: print("C"), 
  "d":    raise_exception, 
  "e":    lambda: print("E") 
}

#dag = DAG()
#dag.from_dict(graph)


predec = dict()

for key in graph:
	nodes = graph[key]
	for node in nodes:
		if node not in predec:
			predec[node] = list()
		predec[node].append(key)

print(predec)

queue = list()
queue.append('root')

has_processed = dict()
failure_jobs = dict()
NO_OF_RETRY = 3

def process_job(ele):
	try:
		processes.get(ele)()
		has_processed[ele] = 1
		nodes = graph[ele]
		for node in nodes:
			queue.append(node)
		return 0
	except Exception as e:
		failure_jobs[ele] = 1
		print("Node has been failed, retry again")
		return 1

while(len(queue) > 0):
	ele = queue.pop(0)
	if ele in has_processed:
		continue
	pre_nodes = predec.get(ele)
	if pre_nodes and len(pre_nodes) > 0:
		can_be_process = 1
		for node in pre_nodes:
			if node not in has_processed:
				can_be_process = 0
				if node not in failure_jobs:
					queue.append(ele)
				break
		if not can_be_process:
			continue

	for retry in range(0, NO_OF_RETRY):
		if not process_job(ele):
			break
