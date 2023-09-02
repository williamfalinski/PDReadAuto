file_dict = {
    'sample1': {
        'columns': {
            'idx' : ['Index'],
            'org_id' : ['Organization Id'],
            'name' : ['Name'],
            'site' : ['Website'],
            'country' : ['Country'],
            'desc' : ['Description'],
            'funded' : ['Founded'],
            'industry' : ['Industry'],
            'employees' : ['Number of employees'],
        }
    },
    'sample1_with_duplicated': {
        'columns': {
            'idx' : ['Index'],
            'org_id' : ['Organization Id'],
            'name' : ['Name'],
            'site' : ['Website'],
            'country' : ['Country'],
            'desc' : ['Description'],
            'funded' : ['Founded'],
            'industry' : ['Industry'],
            'employees' : ['Number of employees(1)'],
        }
    },
    'sample1_with_multiple_duplicated': {
        'columns': {
            'idx' : ['Index'],
            'org_id' : ['Organization Id'],
            'name' : ['Name'],
            'site' : ['Website'],
            'country' : ['Country'],
            'desc' : ['Description'],
            'funded' : ['Founded'],
            'industry' : ['Industry(1)'],
            'industry2' : ['Industry(2)'],
            'employees' : ['Number of employees(1)', 'AAAAAAA1'],
            'employees2' : ['Number of employees(2)'],
        }
    },
}