/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
SELECT c.name category_name, COUNT(fc.film_id) nr_films
FROM public.film_category fc
         INNER JOIN public.category c ON c.category_id = fc.category_id
GROUP BY c.category_id
ORDER BY nr_films DESC;
-- Execution Time: from ~0.8 ms to ~3.5 ms

/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/

-- CTE to calculate the total number of rentals per film
WITH RentalCount AS (SELECT COUNT(*) rents_count, i.film_id, f.title
                     FROM rental r
                              JOIN inventory i ON r.inventory_id = i.inventory_id
                              JOIN film f ON i.film_id = f.film_id
                     GROUP BY i.film_id, f.title
                     ORDER BY rents_count DESC)
-- Main query to calculate the total number of film rentals per actor
SELECT a.actor_id,
       CONCAT(a.first_name, ' ', a.last_name) actor_name,
       SUM(rc.rents_count)                    total_rents
FROM actor a
         JOIN film_actor fa ON a.actor_id = fa.actor_id
         JOIN film f ON fa.film_id = f.film_id
         JOIN RentalCount rc ON rc.film_id = f.film_id
GROUP BY a.actor_id
ORDER BY total_rents DESC
LIMIT 10;
-- Execution Time: from ~10 ms to ~30 ms

-- query without CTE

SELECT a.actor_id,
       CONCAT(a.first_name, ' ', a.last_name) AS actor_name,
       COUNT(*)                               AS total_rents
FROM actor a
         JOIN film_actor fa ON a.actor_id = fa.actor_id
         JOIN inventory i ON fa.film_id = i.film_id
         JOIN rental r ON i.inventory_id = r.inventory_id
GROUP BY a.actor_id
ORDER BY total_rents DESC
LIMIT 10;
-- Execution Time: from ~30 ms to ~45 ms

/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/

SELECT c.name, SUM(p.amount) total_revenue
FROM payment p
         JOIN rental r ON p.rental_id = r.rental_id
         JOIN inventory i ON r.inventory_id = i.inventory_id
         JOIN film_category fc ON fc.film_id = i.film_id
         JOIN category c ON fc.category_id = c.category_id
GROUP BY c.name
ORDER BY total_revenue DESC
LIMIT 1;
-- Execution Time: from ~20 ms to ~40 ms

-- or using CTE

WITH TotalRevenue AS (SELECT fc.category_id,
                             SUM(p.amount) AS total_revenue
                      FROM payment p
                               JOIN rental r ON p.rental_id = r.rental_id
                               JOIN inventory i ON r.inventory_id = i.inventory_id
                               JOIN film_category fc ON fc.film_id = i.film_id
                      GROUP BY fc.category_id)
SELECT c.name,
       tr.total_revenue
FROM category c
         JOIN TotalRevenue tr ON c.category_id = tr.category_id
ORDER BY tr.total_revenue DESC
LIMIT 1;
-- Execution Time: from ~25 ms to ~40 ms


/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/

SELECT f.title
FROM film f
         LEFT JOIN inventory i ON f.film_id = i.film_id
WHERE i.inventory_id IS NULL;
-- Execution Time: from ~1 ms to ~4 ms


/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
-- SQL code goes here...