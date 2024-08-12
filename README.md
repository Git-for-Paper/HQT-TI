# HQT-TI

## 1. Introduction

HQT-TI is a new spatial text indexing structure that consists of two components: the spatial index combining Hilbert curves and QuadTree (HQT) and the materialized inverted index (TI). This index is primarily used for spatial keyword queries (SKQ). The main file includes the construction of the index and related query algorithms (HQT-SQ, SLI-KQ, HS-SKQ).

## 2. Operating Environment

\- **Processor:** Intel i7-7700 @ 3.60GHz 

-**Memory:** 16GB RAM 

-**Operating System:** Windows 10 

-**IDE:** IntelliJ IDEA 2021 

-**JDK:** 1.8

## 3. Project Dependencies

The project depends on the following Java libraries: 

- **dhp_0_7.jar**
- **JavaFastPFOR-0.0.7.jar** 
- **jdbm.jar**
- **mapdb-0.9.1.jar** 
- **mapdb-1.0.9.jar**

##  4. Dataset

### The test dataset must have the following format:

- 1, -86.436719, 32.469271, Apple, Tomato, Yogurt, Cereal, Milk, Ice Cream, Chocolate
- 2, -86.905586, 32.611125, Salt, Cake, Cookies, Vinegar, Beef, Strawberry
- 3, -86.913299, 32.662255, Rice, Oats, Orange, Grape, Butter, Chicken 
- 4, -86.907055, 32.635904, Mung Beans, Tofu, Juice, Beer, Chocolate, Pudding
- 5, -86.907036, 32.632167, Cookies, Milk, Lamb, Fish, Carrot, Cheese
- ...

### The query dataset must have the following format:

- 1, -85.81743703613282, 34.1428214477539, -73.96881013613282, 37.674646747753904, Rice, Oats, Orange, Chicken
- 2, -86.33677755126953, 33.93464960327148, -74.48815065126954, 37.46647490327148, Milk, Lamb, Fish
- 3, -85.90482412109375, 32.370516912841794, -74.05619722109375, 35.902342212841795, Salt, Cake, Cookies, Vinegar, Beef 
- ...

Note: For each query in the query dataset, it is essential to ensure that the frequent keywords (X), total number of keywords (Y), proportion of the query area (P), and the given query conditions are the same.
