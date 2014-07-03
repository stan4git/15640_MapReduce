package dfs;
/**
 * 1. node list - from conf
 * 2. node status monitoring (heart beat, RMI call)
 * 3. load replica number from configuration file
 * 4. hashmap<file name : hashmap<chunk num : hashset<node list>>>
 * 5. dfsScheduler (node picking, file chopping, checkpoint) -- also as stub for client invocation
 * 6. connection mapping
 * 7. registry server
 */
public class NameNode {
	
}
