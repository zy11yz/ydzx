register 'yidian_udf.py'  using jython as yidian_udf;

a = load 'yidian.profile_user_v2' using org.apache.hive.hcatalog.pig.HCatLoader();
b = filter a by p_day == '$P_DAY' and p_type == 'full';
c = filter b by ((user_headfea_off is not null) and (not IsEmpty(user_headfea_off)));
d = foreach c generate user_id, flatten(user_headfea_off.(facet, interest, mean)) as (channel:chararray, interest:chararray, value:double);
i = filter d by channel == '$CAT';
j = foreach i generate interest, user_id, value;
k = filter j by yidian_udf.JudgeCategory(interest,'$INTERESTS');
store k into '$OUTPUT_ALL';
g = foreach k generate interest, user_id;
e = distinct g;
store e into '$OUTPUT';
