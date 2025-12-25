-- access view
-- ToDo:
-- check if current_user() is member of a certain group; mask attributes as needed
select
    t.*
from {{ ref('br_customer') }} t