-- DATA CLEANING

select *
from layoffs;

create table layoffs_work
like layoffs;

insert into layoffs_work
select *
from layoffs;

-- layoffs_work is the workable table I'll use

-- Removing Duplicates

-- creating CTE 

with CTE_layoffs as
(
select *, row_number () 
over(partition by company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) row_num
from layoffs_work)

select *
from CTE_layoffs
where row_num > 1;

create table layoffs_work2
select *, row_number () 
over(partition by company, location, industry, 
total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) row_num
from layoffs_work;

delete
from layoffs_work2
where row_num >1;

-- standardize the data
select distinct country
from layoffs_work2
order by 1;

update layoffs_work2
set company = trim(company);

select *
from layoffs_work2
where country like 'United States';

update layoffs_work2
set country = 'United States'
where country like 'United States%';


select `date`, str_to_date(`date`,'%m/%d/%Y')
from layoffs_work2;

update layoffs_work2
set `date` = str_to_date(`date`,'%m/%d/%Y');

alter table layoffs_work2
modify column `date` date;

delete
from layoffs_work2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_work2
where industry is null or industry =  '';

select *
from layoffs_work2
where company like "Juul";

update layoffs_work2
set industry = null 
where industry = '';

-- self join
select *
from layoffs_work2 lf1
join layoffs_work2 lf2
		on lf1.company = lf2.company
where lf1.industry is null and lf2.industry is not null;


update layoffs_work2 lf1
join layoffs_work2 lf2
		on lf1.company = lf2.company
set lf1.industry = lf2.industry
where lf1.industry is null and lf2.industry is not null;

alter table layoffs_work2
drop column row_num;

# Cleaned Data

select *
from layoffs_work2;
















