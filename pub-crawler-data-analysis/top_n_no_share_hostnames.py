import igraph
import collections

def no_share_hostnames(g):
    followers_members_shared = g.vs["followers_members_shared"]
    following_members_shared = g.vs["following_members_shared"]
    hostname = g.vs["hostname"]
    groups = collections.defaultdict(int)
    for i in range(len(hostname)):
        if hostname[i] is None or hostname[i] == "":
            continue
        if (
            followers_members_shared[i] is None
            or followers_members_shared[i] != followers_members_shared[i]
        ):
            continue
        if (
            following_members_shared[i] is None
            or following_members_shared[i] != following_members_shared[i]
        ):
            continue
        if (
            int(followers_members_shared[i]) == 0
            and int(following_members_shared[i]) == 0
        ):
            groups[hostname[i]] += 1
    return groups


def main(n, gml_filename):
    g = igraph.Graph.Read_GML(gml_filename)
    hostname_to_actor_count = no_share_hostnames(g)
    top_n = collections.Counter(hostname_to_actor_count).most_common(n)
    print(top_n)


if __name__ == "__main__":
    import sys

    n = int(sys.argv[1])
    gml_filename = sys.argv[2]
    main(n, gml_filename)
