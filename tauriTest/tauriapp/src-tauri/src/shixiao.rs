use std::collections::HashMap;
use chrono::{ NaiveDate,Local};
use rand::distr::uniform::SampleBorrow;
use regex::Regex;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, ACCEPT_ENCODING, ACCEPT_LANGUAGE, CONNECTION, ORIGIN, REFERER, USER_AGENT};
use serde_json::{json, Value};


pub struct Apps {
    url: String,
    ku: String,
    users: String,
    header:HeaderMap<HeaderValue>,
    url0:String,
    url1: String,
    url2: String,

}
impl Apps {
    pub fn new(url: &str,users:&str) -> Self {
        Apps {
            url: url.to_string(),
            ku: "crm".to_string(),
            users: users.to_string(),
            header:HeaderMap::new(),
            url0:String::new(),
            url1: String::new(),
            url2: String::new(),

        }
    }
    fn extract_host_id(&self) -> Result<(String, String),Box<dyn std::error::Error>> {
        // 匹配主机部分（非贪婪匹配直到 /#）
        let host_re = Regex::new(r"http://(.*?)/#")?;
        let host = host_re
            .captures(&self.url)
            .and_then(|cap| cap.get(1))
            .map(|m| m.as_str().to_string())
            .ok_or_else(|| "URL 格式错误，无法提取主机")?;

        // 匹配 ID 部分
        let id_re = Regex::new(r"id=([^&]*)")?;
        let id = id_re
            .captures(&self.url)
            .and_then(|cap| cap.get(1))
            .map(|m| m.as_str().to_string())
            .ok_or_else(|| "URL 格式错误，无法提取 ID")?;

        Ok((host, id))
    }

    pub fn head(mut self) -> Result<Self, Box<dyn std::error::Error>> {
        let (host, id) = self.extract_host_id()?;
        self.url0 = format!("http://{}/WebDb/sql/parse", host);
        self.url1 = format!("http://{}/WebDb/operation/exe?id={}&db={}&actuator=editor-actuator&sql=&jk=true", host, id, self.ku);
        self.url2 = format!("http://{}/WebDb/operation/exe?id={}&db={}&actuator=editor-actuator&sql=&jk=&length=1000", host, id, self.ku);

        // 构建请求头
        self.header.clear();
        self.header.insert(ACCEPT, HeaderValue::from_static("application/json, text/plain, */*"));
        self.header.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip, deflate"));
        self.header.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("zh-CN,zh;q=0.9,ja;q=0.8"));
        self.header.insert(CONNECTION, HeaderValue::from_static("keep-alive"));
        self.header.insert(ORIGIN, HeaderValue::from_str(&format!("http://{}", host))?);
        self.header.insert(REFERER, HeaderValue::from_str(&format!("http://{}/", host))?);
        self.header.insert(
            USER_AGENT,
            HeaderValue::from_static("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"),
        );

        Ok(self)
    }

    async fn get_data(&self, sql: &str) -> Result<Vec<HashMap<String, Value>>,Box<dyn std::error::Error>> {
        let client = reqwest::Client::new();
        // 第一个请求 (url0)
        let data0 = json!({"sql": sql.replace("\n"," "), "type": "sql"});
        let _res0 = client
            .post(&self.url0)
            .headers(self.header.clone())
            .json(&data0)
            .send()
            .await?;

        // 第二个请求 (url1)
        let data = json!({"sql": sql.replace("\n"," ")});
        let _res1 = client
            .post(&self.url1)
            .headers(self.header.clone())
            .json(&data)
            .send()
            .await?;

        // 第三个请求 (url2)
        let res2 = client
            .post(&self.url2)
            .headers(self.header.clone())
            .json(&data)
            .send()
            .await?;

        // 解析响应
        let response_text = res2.text().await?;

        let json_response: Value = serde_json::from_str(&response_text)?;

        let result = json_response.get("data")
            .and_then(|v| v.as_array())
            .and_then(|v|v.get(0))
            .and_then(|v|v.get("data"))
            .and_then(|v|v.get("data"))
            .and_then(|v| v.as_array())
            .unwrap_or(&vec![])
            .iter()
            .map(|item| {
                let row = item.as_object().unwrap();
                row.iter()
                    .map(|(k, v)| (k.to_string(), v.clone()))
                    .collect::<HashMap<String, Value>>()
            })
            .collect();

        Ok(result)
    }

    pub async fn get_crm_data(&self) -> Result<Vec<HashMap<String, String>>,Box<dyn std::error::Error>> {
        let sql= format!("select a.user_id,max(b.end_date) as end_date,a.USER_STATE ,MOD(left(a.PARTITION_ID,14),8)+1 AS ku, MOD(a.PARTITION_ID,10)+1 biao1,MOD(a.PARTITION_ID,100)+1 biao2 from tf_f_user  a left join tf_f_user_product b on a.user_id=b.user_id where a.user_id in ({})group by a.user_id",&self.users);

        let data=&self.get_data(&sql).await?;

        let result:Vec<HashMap<String,String>> = data.iter().map(|x|
            {
                let user_id = x.get("USER_ID").unwrap().as_str().unwrap();
                let end_date = x.get("END_DATE").unwrap().as_str().unwrap();
                let ku = x.get("KU").unwrap().as_str().unwrap().trim_end_matches(".0");
                let biao = x.get("BIAO1").unwrap().as_str().unwrap().trim_end_matches(".0");
                let date = Local::now().naive_local().date();
                let sql_user=format!("update crm{0}.tf_f_user_{1} set user_state='9',remove_tag='2',update_time=now(),remark='{3}失效资源' where user_id={2};",ku,biao,user_id,date);
                let end_date2 = NaiveDate::parse_from_str(end_date, "%Y-%m-%d %H:%M:%S").expect("解析日期失败");
                let sql_product= match  end_date2{
                    v if v > date => format!("update crm{0}.tf_f_user_product_{1} set end_date=now(),update_time=now(),remark='{3}失效资源' where user_id={2};",ku,biao,user_id,date),
                    _=> "".to_string()
                };
                let sql_discnt= match  end_date2{
                    v if v > date => format!("update crm{0}.tf_f_user_discnt_{1} set end_date=now(),update_time=now(),remark='{3}失效资源' where user_id={2};",ku,biao,user_id,date),
                    _=> "".to_string()
                };
                let sql_bill= match  end_date2{
                    v if v > date => format!("update jour.tf_b_fastqry_phonebill set INSTANCE_STATE='9',EXP_TIME=now(),UPDATE_TIME=now() where user_id={0};",user_id),
                    _=> format!("update jour.tf_b_fastqry_phonebill set INSTANCE_STATE='9',UPDATE_TIME=now() where user_id={0};",user_id)
                };
                let sql_op= match  end_date2{
                    v if v > date => format!("update jour.tf_b_oporderquery set ITEM_TYPE='2',STATUS='20',END_DATE=now(),UPDATE_TIME=now() where mop_user_id={0};",user_id),
                    _=> format!("update jour.tf_b_oporderquery set ITEM_TYPE='2',STATUS='20',UPDATE_TIME=now() where mop_user_id={0};",user_id)
                };

                let last=HashMap::from([
                    ("sql_user".to_string(), sql_user),
                    ("sql_product".to_string(), sql_product),
                    ("sql_discnt".to_string(), sql_discnt),
                    ("sql_bill".to_string(), sql_bill),
                    ("sql_op".to_string(), sql_op),
                ]);
                println!("last  {:?}",last);
                last
            }
        ).collect();
        Ok(result)
    }

}



#[tokio::main]
async  fn main() -> Result<(),Box<dyn std::error::Error>>{
    let url_crm= "http://10.174.76.61:2001/#/?id=MTg0MTc2Nzc2MjcyMDIyMDI2My0xNzU2NDQ2NjEyMDkyLU1ZU1FM";
    let users="24110527371657,24110527371663,24112827439967";
    let  apps = Apps::new(url_crm,users);
    let data=apps.head()?.get_crm_data().await?;
    println!("{:#?}", data);
    Ok(())

}
