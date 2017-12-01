/*
 * master.h
 *
 *  Created on: Nov 28, 2017
 *      Author: anadioti
 */

#ifndef MASTER_H_
#define MASTER_H_

void start_hash_table_servers(struct hash_table *hash_table);
void start_hash_table_servers_hotplug(struct hash_table *hash_table, int hotplugged_servers);


#endif /* MASTER_H_ */
