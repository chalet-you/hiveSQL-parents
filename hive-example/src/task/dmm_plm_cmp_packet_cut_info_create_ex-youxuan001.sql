use ph_rc_dmm;
drop table if exists ph_rc_dmm.dmm_plm_cmp_packet_cut_info;
create table if not exists ph_rc_dmm.dmm_plm_cmp_packet_cut_info
(
apply_no                    string                  comment '申请号',
cut_pkg_time                string                  comment '切包时间',
biz_type                    string                  comment '业务类型',
sales_org_id                string                  comment '案件归属机构ID',
sales_org_name              string                  comment '案件归属机构',
plb_pkg_org_id_lmt          string                  comment '迁移前贷后归属ID',
plb_pkg_org_name_lmt        string                  comment '迁移前贷后归属',
plb_pkg_org_id              string                  comment '迁移后贷后归属ID',
plb_pkg_org_name            string                  comment '迁移后贷后归属',
loan_bal                    decimal(38, 10)         comment '剩余本金',
tag_type                    string                  comment '标签类型',
risk_level                  string                  comment '风险等级',
create_by                   string                  comment '创建人',
date_created                timestamp               comment '创建时间',
updataed_by                 string                  comment '更新人',
date_updated                timestamp               comment '更新时间'
)
comment 'cmp切包分案信息表'
partitioned by (stat_date string comment '统计日')
row format delimited fields terminated by '\u0001'
stored as orc;