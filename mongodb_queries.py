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

