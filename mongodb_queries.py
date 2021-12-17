qry1 =[
    {
        "$group": {
            '_id': {
                'country': '$country'
            }, 
            'killed': {
                '$sum': '$total_killed'
            }
        }
    }, {
        '$sort': {
            'killed': -1
        }
    }
]

qry2 =[
    {
        '$lookup': {
            'from': 'target_dim', 
            'localField': 'target', 
            'foreignField': 'target', 
            'as': 'target_inf'
        }
    }, {
        '$lookup': {
            'from': 'event_dim', 
            'localField': 'event_id', 
            'foreignField': 'event_id', 
            'as': 'event_inf'
        }
    }, {
        '$project': {
            'target_inf': {
                '$arrayElemAt': [
                    '$target_inf', 0
                ]
            }, 
            'event_inf': {
                '$arrayElemAt': [
                    '$event_inf', 0
                ]
            }, 
            'total_killed': '$total_killed'
        }
    }, {
        '$group': {
            '_id': '$target_inf.target_nat', 
            'ransom': {
                '$sum': '$event_inf.ransom_amt'
            }, 
            'ransom_paid': {
                '$sum': '$event_inf.ransom_amt_paid'
            }, 
            'killed': {
                '$sum': '$total_killed'
            }
        }
    }, {
        '$match': {
            'ransom': {
                '$gt': 0
            }
        }
    }, {
        '$sort': {
            'ransom': -1
        }
    }
]

qry3=[
    {
        '$group': {
            '_id': {
                'country': '$country', 
                'group': '$group_name'
            }, 
            'n_attacks': {
                '$count': {}
            }, 
            'killed': {
                '$sum': '$total_killed'
            }, 
            'prop_dmg': {
                '$sum': '$property_damage'
            }
        }
    }, {
        '$sort': {
            'n_attacks': -1
        }
    }, {
        '$project': {
            'Country': '$_id.country', 
            'Group Name': '$_id.group', 
            'Nr of Attacks': '$n_attacks', 
            'Total Killed': '$killed', 
            'Property Damage': '$prop_dmg', 
            '_id': 0
        }
    }
]