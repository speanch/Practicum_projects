-- Задача 1: Время активности объявлений
-- Определим аномальные значения (выбросы) по значению перцентилей:
WITH limits AS (
    SELECT  
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
        PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats     
),
-- Найдем id объявлений, которые не содержат выбросы:
filtered_id AS(
    SELECT id
    FROM real_estate.flats  
    WHERE 
        total_area < (SELECT total_area_limit FROM limits) 
        AND rooms < (SELECT rooms_limit FROM limits) 
        AND balcony < (SELECT balcony_limit FROM limits) 
        AND ceiling_height < (SELECT ceiling_height_limit_h FROM limits) 
        AND ceiling_height > (SELECT ceiling_height_limit_l FROM limits)
    ),
-- Выведем объявления без выбросов:
filtered_flats AS (
	SELECT *
	FROM real_estate.flats
	WHERE id IN (SELECT * FROM filtered_id)),
regions_data AS (
	SELECT 
	CASE
		WHEN city = 'Санкт-Петербург' THEN 'Санкт-Петербург'
		ELSE 'ЛенОбласть'
	END AS region, -- Выделяем регионы 
	CASE 
		WHEN a.days_exposition > 1 AND a.days_exposition <=30 THEN 'до месяца'
		WHEN a.days_exposition > 31 AND a.days_exposition <=90 THEN 'до трех месяцев'
		WHEN a.days_exposition > 91 AND a.days_exposition <=180 THEN 'до полугода'
		WHEN a.days_exposition > 181 THEN 'больше полугода'
	END AS activity_segment, -- Выделяем временные интервалы
	CASE 
		WHEN a.days_exposition > 1 AND a.days_exposition <=30 THEN 1
		WHEN a.days_exposition > 31 AND a.days_exposition <=90 THEN 2
		WHEN a.days_exposition > 91 AND a.days_exposition <=180 THEN 3
		WHEN a.days_exposition > 181 THEN 4
	END AS rank_activity_segment, -- Присваеваем ранг временным интервалам
	a.last_price / ff.total_area AS cost_m2, -- Считаем стоимость одного кв. метра 
	ff.total_area, ff.rooms, ff.balcony, ff.floor -- Выделяем необходимые параметры по каждому объявлению
FROM filtered_flats AS ff
LEFT JOIN real_estate.city c USING (city_id)
LEFT JOIN real_estate.advertisement a USING (id)
LEFT JOIN real_estate."type" t USING (type_id)
WHERE a.days_exposition IS NOT NULL
AND t.TYPE = 'город' -- добавлена фильтрация по городам
)
-- ОСНОВНОЙ ЗАПРОС
SELECT region, activity_segment, -- Выбираем регионы и временные интервалы 
	round(AVG(cost_m2::NUMERIC),2) AS avg_cost, -- Расчитываем среднюю стоимость одного кв.м
	round(avg(total_area::NUMERIC),2) AS avg_total_area, -- Расчитываем среднюю площадь
	percentile_cont(0.5) WITHIN GROUP (ORDER BY rooms) AS median_rooms, -- Медиана количества комнат
	percentile_cont(0.5) WITHIN GROUP (ORDER BY balcony) AS median_balcony, -- Медиана количества балконов
	percentile_cont(0.5) WITHIN GROUP (ORDER BY floor) AS median_floor, -- Медиана количества этажей
	round((avg(cost_m2::NUMERIC)*avg(total_area::NUMERIC)),2) AS avg_total_cost -- Средняя стоимость квартиры (доп расчет)
FROM regions_data
WHERE activity_segment IS NOT NULL
GROUP BY 1,2, rank_activity_segment
ORDER BY 1 DESC, rank_activity_segment;

-- Задача 2: Сезонность объявлений
WITH month_exposition AS (
	SELECT id, 
		last_price / total_area AS cost_m2,
		total_area,
		EXTRACT(MONTH FROM first_day_exposition) AS first_month_exposition,
		EXTRACT(MONTH FROM (first_day_exposition + INTERVAL '1 day' * days_exposition)) AS last_month_exposition
	FROM real_estate.flats
	LEFT JOIN real_estate.advertisement a USING(id)
	LEFT JOIN real_estate."type" t USING(type_id)
	WHERE date_trunc('month', first_day_exposition) > '01.11.2014' AND date_trunc('month', first_day_exposition) < '01.04.2019'
	AND t.TYPE = 'город' -- добавлена фильтрация по городам
	),
	first_month AS (
	SELECT first_month_exposition AS month, 
		count(id) AS new_flats,
		round(avg(total_area::NUMERIC),2) AS avg_area_new_flats,
		round(avg(cost_m2::NUMERIC),2) AS cost_m2_new_flats
	FROM month_exposition
	GROUP BY 1 ORDER BY 1
	),
	last_month AS (
	SELECT last_month_exposition AS month, 
		count(id) AS sold_flats,
		round(avg(total_area::NUMERIC),2) AS avg_area_sell_flats,
		round(avg(cost_m2::NUMERIC),2) AS cost_m2_sell_flats
	FROM month_exposition
	GROUP BY 1 ORDER BY 1
	)
-- ОСНОВНОЙ ЗАПРОС	
SELECT 
	month, -- месяц
	new_flats, -- новые объявления
	sold_flats,  -- старые объявления
	RANK() OVER(ORDER BY new_flats DESC) AS rank_month_new, -- ранг новых объявлений
	cost_m2_new_flats, avg_area_new_flats, -- стоимость кв.м и средняя площадь новых объявлений
	RANK() OVER(ORDER BY sold_flats DESC) AS rank_month_sell, -- ранг для снятых объявлений
	cost_m2_sell_flats, avg_area_sell_flats, -- стоимость кв.м и средняя площадь снятых объявлений
	round((new_flats::NUMERIC / (new_flats + sold_flats)),2) AS share_new_flats, -- доля новых объявлений
	round((sold_flats::NUMERIC / (new_flats + sold_flats)),2) AS share_sold_flats -- доля снятых объявлений
FROM first_month
JOIN last_month USING(month)
ORDER BY MONTH;

-- Задача 3: Анализ рынка недвижимости Ленобласти
-- Определим аномальные значения (выбросы) по значению перцентилей:
WITH limits AS (
    SELECT  
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
        PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats     
),
-- Найдем id объявлений, которые не содержат выбросы:
filtered_id AS(
    SELECT id
    FROM real_estate.flats  
    WHERE 
        total_area < (SELECT total_area_limit FROM limits) 
        AND rooms < (SELECT rooms_limit FROM limits) 
        AND balcony < (SELECT balcony_limit FROM limits) 
        AND ceiling_height < (SELECT ceiling_height_limit_h FROM limits) 
        AND ceiling_height > (SELECT ceiling_height_limit_l FROM limits)
    ),
-- Выведем объявления без выбросов:
filtered_flats AS (
	SELECT *
	FROM real_estate.flats
	WHERE id IN (SELECT * FROM filtered_id)	
	),
-- Выведенем метрики для объявлений по регионам
regions_data AS (
	SELECT a.id, c.city, a.days_exposition,
		a.last_price / ff.total_area AS cost_m2, -- Считаем стоимость одного кв. метра 
		ff.total_area -- Выделяем необходимые параметры по каждому объявлению
		FROM filtered_flats AS ff
	LEFT JOIN real_estate.city c USING (city_id)
	LEFT JOIN real_estate.advertisement a USING (id)
	WHERE city <> 'Санкт-Петербург' -- фильтр по городам не относящимся к Санкт-Петербургу
	),
-- Считаем количество объявлений, которые еще не продали
sold_flats AS (
	SELECT city, count(id) AS count_sold_flats
	FROM regions_data
	WHERE days_exposition IS NOT NULL
	GROUP BY city
	),
-- Считаем общее количество объявлений по регионам + средние метрики регионов
all_flats AS (
	SELECT city, -- регион
		count(id) AS count_all_flats, -- количество объявлений в регионе
		round(avg(cost_m2::NUMERIC),2) AS avg_cost_m2, -- средняя стоимость одного кв. метра
		round(avg(total_area::numeric),2) AS avg_total_area, -- средняя площадь квартиры
		round(avg(days_exposition::numeric),0) AS avg_days_exposition -- средняя продолжительность объявлени
	FROM regions_data
	GROUP BY city
	)
-- ОСНОВНОЙ ЗАПРОС
SELECT city, -- регион
	count_all_flats, -- количество объявлений в регионе
	round((count_sold_flats::NUMERIC / count_all_flats),2) AS share_sold_flats,
	avg_cost_m2, -- средняя стоимость одного кв. метра
	avg_total_area, -- средняя площадь квартиры
	avg_days_exposition -- средняя продолжительность объявления
--	,(SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY count_all_flats) FROM all_flats) -- доп. расчет для выбора граничного значения
	,ntile(6) OVER(ORDER BY count_all_flats DESC) -- добавил ранжирование 
FROM all_flats
LEFT JOIN sold_flats USING(city)
WHERE count_sold_flats IS NOT NULL
AND count_all_flats >= 20
ORDER BY count_all_flats DESC

