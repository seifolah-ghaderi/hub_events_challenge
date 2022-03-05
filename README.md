# The Hub  events challenge
Achievements: Calculate two below metrics for hub supplier
- supplier average review 
- supplier acceptance ratio

Author: *Seifolah Ghaderi*
Repo: https://github.com/seifolah-ghaderi/hub_events_challenge
### Assumptions:
- A postgres database is up and running on localhost and port 5432
- A database with name `postgres` is ready with following credentials: 
  - user: `postgres`
  - pwd: `pass`
- Two folder exists in code:
  - sql : contains `hubs_events.sql` that contain table definition and  `raw` initial data 
  - csv : the data loaded first time are saved here to load from any jupyter notebook or other execution when database is not run.

### Customize database
if you have your own database you should change the following lines in `main.py` with your credential :
- *db_conn* global variable: 
```
db_conn = 'postgresql+psycopg2://postgres:pass@127.0.0.1'
````
- in function *delete_today_records* :
```
conn = psycopg2.connect(database="postgres", user='postgres', password='pass', host='127.0.0.1', port='5432')
```

### Prepare and run code

#### Database 
you can use docker-compose to run a postgres database and automatically load sql script.
Be sure that you are in code folder and then run:
  
 ```
   docker-compose up -d
```
be sure that postgres is run by checking:
```
docker ps
```

#### running code:

This is a pure python code so just make sure you have created a virtual environment and then run code by :

`cd Folder(source code):`
```
python3 -m venv venv  
source venv/bin/activate 
python main.py
```

### output
The output for each metric will be print in `console` window(in vscode or pycharm).
and the table `supplier_score_metrics` will be created and populated in database.

You can query table by running :
```commandline
SELECT supplier_id, value, metric, calculated_at
FROM public.supplier_score_metrics;
```

# Milestones:
- load dataframe from csv( or from postgres table)
- calculate acceptance ratio metric
- calculate average review metric
- prepare to be ready for insert in database
- write them to metric table


## Supplier Metrics:
- Average review 
- Acceptance ratio 
- Response time (suggested by me)
- Deliver time (suggested by me)
#### Acceptance ratio percentage:
```
 ( accepted aorders / total  assigned orders) * 100
```
- `assigned orders` could be find in event: `order/execute/customer/status/processing`
- `accepted orders`  could be find in event : `order/execute/customer/status/payment`
### functions & processes
`parser & extract  ` --> `calculate aggregate` --> `calculate metrics `

- `1:parser & extractor` functions:
```commandline
- get_assigned_orders 
- get_accepted_orders

```
Those above function get `df` as master dataframe and by searching relative `event` extract json data and convert it to dataframe .
```commandline
df_mask = df['name'] == 'order/execute/customer/status/processing'
df_mask = df['name'] == 'order/execute/customer/status/payment'
```
`extraxt and convert to datafram` done using the `pd.json_normalize` function.

- `2:calculate aggregate` functions:
```commandline
- cal_all_order_count 
- cal_accepted_orders_count
```
Those two above function calculate `total` & `accepted` orders for each `supplier` using dataframe returned from parser functions.
```commandline
df_all.groupby(['supplier_id']).count()
```

- `3:main function`
```commandline
cal_accept_ratio
```
this is the final function that calculate acceptance ratio for each supplier.
```commandline
    df_all_o = get_assigned_orders(df)
    df_accepteds_o = get_accepted_orders(df)

    df_all = cal_all_order_count(df_all_o)
    df_accepteds = cal_accepted_orders_count(df_accepteds_o)

    ratio_df = pd.merge(df_all, df_accepteds)

    ratio_df['value'] = round((ratio_df['accepted'] / ratio_df['total']) * 100).astype(int)
```


### Average review metric
Contains two metrics: 
```commandline
- review_value_speed
- review_value_print_quality
```
These two metric are available in *'node/review/created'* `events` plus some modification and deleted row in *'node/review/updated'* & *'node/review/deleted'* `evenets`.
###challenges :
`updated` and `removed` reviews should be considered.
So we developed three functions:
```commandline
- get_all_reviews
- get_upd_reviews
- get_del_reviews
```
These functions  in order : get all review , updated and deleted reviews.
After getting them we update reviews by apply `modified` rows and also `delete` removed reviews.
The function that does the main calculation is :
```commandline
cal_review
```
I calculated both `review_value_speed` and `review_value_print_quality` as two separated metrics  for each supplier.
And then make a new column to calculate their average .These all done in `cal_review` function.
```
  avg ( review_value_speed ) + avg(review_value_print_quality)/ 2
```
```commandline
    grouped_df = df_reviews.groupby(['supplier_id']).mean().astype(int)
    grouped_df['value'] = round((grouped_df['review_speed'] + grouped_df['review_quality']) / 2).astype(int)
```
### Checking data
- Update `event` with missed `order_id` :

What i found during checking data is that there are some updated review that not exists in `created` reviews !.maybe they belong to another time windows. 
For example below record not exists in `created` event but exists in `updated` event:
```commandline
event	            supplier_id	order_id	    timestamp	            customer_id	    review_speed	review_quality
node/review/updated	34872	    3194344	    2017-01-03T22:10:58.000Z	  154527	        100	            80          
```
- multiple review for single order!:
during removing `deleted reviewes` I found that there are multiple reviews from one `customer_id`   for a single `order` and  `supplier` !
```commandline
event	                supplier_id	order_id	    timestamp	            customer_id	    review_speed	review_quality
node/review/deleted	    30352	    3994815	    2017-01-11T08:37:47.000Z	51468	        100	                100
node/review/deleted	    30352	    3994815	    2017-01-11T08:28:40.000Z	51468	        100	                100
```
This will be meaning full when you decide to delete reviews by only mathing `order_id`.I had 5 deleted review and when i deleted their `order_id` from `reviews` it removed 6 records indicate that we have multiple `order_id` 
for resolve it i included `customer_id` in filtering and removing reviews and then it removed exactly as i expected, 5 rows~


```commandline
cond = df_reviews['order_id'].isin(df_del_reviews['order_id']) & df_reviews['customer_id'].isin(
        df_del_reviews['customer_id'])
    df_reviews.drop(df_reviews[cond].index, inplace=True)
```
# Extra Metrics for Suppliers
- Average Response time (suggested by me) : avg (`accepted_time` - `assigned time`)
- Average Deliver time (suggested by me) : avg (`delivered time` - `start time`)

## Average Response time:
- `assigned time` : When customer assign an order to a supplier during *'order/execute/customer/status/processing'* `event`
- `accepted time` : When supplier review and accept an order    during *'order/execute/customer/status/payment'* `event`
The `Supplier Average Response time` indicate how much a supplier is timely and punctual or how much he respects customer time !
This metric calculated in `hour` and when you lok at the result it's interesting that range of response to an order is varies form less than one hour to 16h !
```commandline
	supplier_id	avg_respon_h
0	100130	        0
1	114087	        0
2	170225	        0
3	62909	        1
4	18815	        2
5	192417	        2
6	34872	        3
7	84241	        8
8	30352	        11
9	4450	        13
10	69607	        16

```
The function `cal_sup_resp_time` used for calculate this metric.

## Average Deliver time:
- `start time` : When supplier start manufacturing  during *'order/execute/status/customer/printing'* `event`
- `deliver time` : When supplier finished order and order delivered to customer  during *'order/execute/status/customer/successful'* `event`

Unfortunately i couldn't find any event `order/execute/status/customer/successful` in the provided data and can not calculate this metrics. But the process is same as `Average Response time` metric.
