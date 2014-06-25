from collections import defaultdict
from recordtype import recordtype
import copy
import sys
import csv
import threading

ClusterLink = recordtype('Link', 'count records')

class Cluster():

    # Static class variable for cluster links
    links = defaultdict(lambda: defaultdict(lambda: ClusterLink(0, set([]))))

    ips = set()
    records = defaultdict(int)
    internal_connections = 0
    _next_id = 0
    _id_lock = threading.RLock()

    @classmethod
    def _new_id(cls):
        with cls._id_lock:
            new_id = cls._next_id
            cls._next_id += 1
        return new_id

    def __init__(self, ips, internal_connections):
        self.id = self._new_id()
        self.ips = ips
        self.internal_connections = internal_connections
        self.records = defaultdict(int)

    def __str__(self):
        return_str = "\n\nCluster "+str(self.id) +\
                     "\n============\n"\
                     + str(len(self.ips))+" users:\n"

        for ip in self.ips:
            return_str += ip + ", "

        return_str += "\n\n" + str(self.internal_connections) + " internal connections"
        self_links = Cluster.get_links(self.id)
        if len(self_links) > 0:
            return_str += "\n\nConnected clusters: "
            for id, strength in self_links.iteritems():
                return_str += str(id) + " (" + str(strength) + "), "
        return_str += "\n\n"+str(len(self.records))+" records"
        return_str += "\nMost popular:\n"
        self_sorted_records = sorted(self.records.items(), key=lambda x: x[1], reverse=True)
        counter = 0
        for record_id, count in self_sorted_records:
            if counter > 14:
                break
            return_str += str(record_id) + " (" + str(count) + "), "
            counter += 1

        return return_str

    def add_ip(self, new_ip):
        self.ips.append(new_ip)

    @classmethod
    def add_link(cls, cl1, cl2, records):
        cls.links[cl1][cl2].count += 1
        cls.links[cl1][cl2].records.update(records)

    @classmethod
    def get_links(cls, start_id):
        links_dict = defaultdict(lambda: ClusterLink(0, set([])))

        if start_id in cls.links:
            for linked_id, cluster_link in cls.links[start_id].iteritems():
                links_dict[linked_id] = cluster_link

        for link_id, linked_ids in cls.links.iteritems():
            if start_id in linked_ids:
                links_dict[link_id] = cls.links[link_id][start_id]

        return links_dict

    @classmethod
    def update_links(cls, from_id, to_id):
        new_link_dict = copy.deepcopy(cls.links)

        for id_1, ids in cls.links.iteritems():

            if id_1 == from_id:
                new_link_dict[to_id].update(cls.links[from_id])
                del new_link_dict[from_id]
                continue

            for id_2, count in ids.iteritems():
                if id_2 == from_id:
                    new_link_dict[to_id][id_1].count += cls.links[id_1][id_2].count
                    new_link_dict[to_id][id_1].records.update(cls.links[id_1][id_2].records)

                    del new_link_dict[id_1][id_2]

        cls.links = new_link_dict


class defaultlist(list):
    def __init__(self, fx):
        self._fx = fx

    def __setitem__(self, index, value):
        while len(self) <= index:
            self.append(self._fx())
        list.__setitem__(self, index, value)


def assign_to_cluster(cluster_objects, ip1, ip2, records):
    # Check every cluster to see if they contain the IP
    ip1_cluster = -1
    ip2_cluster = -1
    for cluster_object in cluster_objects:
        # Any of the IPs in the cluster? If not, continue
        if not ip1 in cluster_object.ips and not ip2 in cluster_object.ips:
            continue

        if ip1 in cluster_object.ips:
            ip1_cluster = cluster_object.id

            # If both are already in cluster, add records, +1 to internal connections and return
            if ip2 in cluster_object.ips:
                for record in records:
                    cluster_object.records[record] += 1

                cluster_object.internal_connections += 1
                return

        if ip2 in cluster_object.ips:
            ip2_cluster = cluster_object.id

    # IPs are found in separate clusters. Add a link
    if ip1_cluster >= 0 and ip2_cluster >= 0:
        Cluster.add_link(ip1_cluster, ip2_cluster, records)

        # Add records to both clusters
        for record in records:
            cluster_objects[ip1_cluster].records[record] += 1
            cluster_objects[ip2_cluster].records[record] += 1

        # Also, merge clusters if the link is stronger than five
        merge_clusters(cluster_objects, ip1_cluster, ip2_cluster, 5)

    # IP1 found. Add IP2 to its cluster
    elif ip1_cluster >= 0:
        cluster_objects[ip1_cluster].add_ip(ip2)
        for record in records:
            cluster_objects[ip1_cluster].records[record] += 1
        cluster_objects[ip1_cluster].internal_connections += 1

    # IP2 found. Add IP1 to its cluster
    elif ip2_cluster >= 0:
        cluster_objects[ip2_cluster].add_ip(ip1)
        for record in records:
            cluster_objects[ip2_cluster].records[record] += 1
        cluster_objects[ip2_cluster].internal_connections += 1

    # None of the IPs are found. Create new cluster
    else:
        new_cluster = Cluster([ip1, ip2], 1)
        for record in records:
            new_cluster.records[record] += 1
        cluster_objects[new_cluster.id] = new_cluster


def merge_clusters(cluster_objects, ip1_cluster, ip2_cluster, limit):
    ip1_ip2_links = Cluster.get_links(ip1_cluster)[ip2_cluster]

    if ip1_ip2_links.count < limit:
        return False

    cluster1 = cluster_objects[ip1_cluster]
    cluster2 = cluster_objects[ip2_cluster]

    # Add all IPs from cluster 2 to cluster 1
    cluster1.ips = cluster2.ips

    # Add all records from cluster 2 to cluster 1
    for record in cluster2.records:
        cluster1.records[record] += 1

    # Add the common records to cluster 1
    for record in ip1_ip2_links.records:
        cluster1.records[record] += 1

    # Increase the number of internal connections to the sum plus the number of links
    # between the clusters
    cluster1.internal_connections += (cluster2.internal_connections + ip1_ip2_links.count)

    # Change all cluster-links to reflect the merge
    Cluster.update_links(cluster2.id, cluster1.id)

    return True


def create_clusters(input_file):
    try:
        log = open(input_file, 'r')
    except IOError:
        print "Error: can\'t find file or read data: "+input_file+"\n"
        sys.exit()
    log_reader = csv.DictReader(log, delimiter=",")


    # Create the data structures
    ip_recs = defaultdict(set)
    rec_ips = defaultdict(set)
    pruned_ip_recs = defaultdict(set)
    pruned_rec_ips = defaultdict(set)
    ip_pairs = defaultdict(lambda: defaultdict(set))
    clusters = defaultlist(lambda: Cluster)

    print "Creating sets..."
    for line in log_reader:
        ip_recs[line['IP']].add(line['Record'])
        rec_ips[line['Record']].add(line['IP'])

    print "Pruning sets..."
    for ip, records in ip_recs.iteritems():
        if len(records) > 5:
            pruned_ip_recs[ip] = records
    for record, ips in rec_ips.iteritems():
        if len(ips) > 5:
            pruned_rec_ips[record] = ips

    print "\nBuilding IP relationships..."
    counter = 0
    for ip, records in pruned_ip_recs.iteritems():
        for record in records:
            if ip in pruned_rec_ips[record]:
                for rel_ip in pruned_rec_ips[record]:
                    if ip != rel_ip:
                        ip_pairs[ip][rel_ip].add(record)

        counter += 1
        sys.stdout.write("\rProcessed " + str(counter) + " of " + str(len(pruned_ip_recs)) + " ips")
        sys.stdout.flush()


    # Sort the ip pairs by amount of shared records, to make sure ips aren't "stolen" by
    # lesser clusters
    #print "Sorting IP pairs"
    #for ip1, ips in ip_pairs.iteritems():
        #sorted_ips = sorted(ips.items(), key=lambda x: len(x.get))


    print "\n\nCreating clusters..."
    pair_length = len(ip_pairs)
    counter = 0
    for ip, rel_ips in ip_pairs.iteritems():
        for rel_ip, records in rel_ips.iteritems():

            # If the IP <-> IP relationship has less than 3 shared records, don't cluster them
            if len(records) < 3:
                break
            # Check every cluster to see if they contain the IP
            assign_to_cluster(clusters, ip, rel_ip, records)

        counter += 1
        sys.stdout.write("\rProcessed " + str(counter) + " of " + str(pair_length) + " ip pairs")
        sys.stdout.flush()

    print "\n\nFound %d clusters" % len(clusters)

    # Remove clusters with less than 40 ips
    pruned_clusters = []
    ip_lm = 1
    for id, cluster in enumerate(clusters):
        if len(cluster.ips) >= ip_lm:
            pruned_clusters.append(cluster)

    print "Removed " + str(len(clusters) - len(pruned_clusters)) + \
          " clusters (less than " + str(ip_lm) + " users)"

    # Print the resulting clusters
    sorted_pruned_clusters = sorted(pruned_clusters, key=lambda x: len(x.ips), reverse=True)
    return sorted_pruned_clusters


def main():
    for cluster in create_clusters(sys.argv[1]):
        print cluster

if __name__ == "__main__": main()