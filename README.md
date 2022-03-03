# The Hub  events challenge
Achievements:
- average review 
- acceptance ratio 

#overall processes:
- load dataframe from csv( or from postgres table)
- calculate acceptance ratio
- calculate average review
- write them to metric table (with current data as calculated time)
### Acceptance ratio:
```
for each supplier: all ordere assigned / orders accepted
```
*order assigned*: could be find in event `order/execute/customer/status/processing`
*order accepted*: could be find in event `order/execute/customer/status/payment`

The common key in two above dataframe is supplier id, we can join them to calculate `acceptance ratio`


#### Average review: 
```
For each supplier:  sum (all reviews)/ number of reviews
```
But as we have updated reviews & deleted reviews 2 step should be done as `data cleaning` before last assessment:
- update modified reviews 
- remove deleted reviews 

