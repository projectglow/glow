# Copyright 2019 The Glow Authors
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import sys

# For each region in a space-separated file containing genes and associated variants, prints an association result
# if all variants for a gene are seen in a VCF.

gene_reqd_sites_dict = {}
region_file = sys.argv[1]

with open(region_file) as r:
    line = r.readline()
    while line:
        split_line = line.split()
        gene = split_line[0]
        sites = split_line[1:]
        gene_reqd_sites_dict[gene] = sites
        line = r.readline()

gene_seen_sites_dict = {}

for line in sys.stdin:
    if not line.startswith('#'):
        split_line = line.split('\t')
        chr = split_line[0]
        pos = split_line[1]
        site = '{}:{}'.format(chr, pos)
        for gene, reqd_sites in gene_reqd_sites_dict.items():
            if site in reqd_sites:
                if gene in gene_seen_sites_dict:
                    gene_seen_sites_dict[gene].append(site)
                else:
                    gene_seen_sites_dict[gene] = [site]

any_genes_seen = False
for gene, reqd_sites in gene_reqd_sites_dict.items():
    if set(reqd_sites) == set(gene_seen_sites_dict[gene]):
        if not any_genes_seen:
            print('Gene pValue')
        any_genes_seen = True
        print('{} 0.5'.format(gene))
