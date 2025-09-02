use anyhow::{anyhow, Context, Result};
use chrono::Local;
use rand::Rng;
use regex::Regex;
use reqwest::header::{
    HeaderMap, HeaderValue, ACCEPT, ACCEPT_ENCODING, ACCEPT_LANGUAGE, CONNECTION, ORIGIN, REFERER,
    USER_AGENT,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::error::Error;

struct Apps {
    url: String,
    ku: String,
}
impl Apps {
    fn new(url: &str, ku: &str) -> Self {
        Apps {
            url: url.to_string(), // 内部转换为 String
            ku: ku.to_string(),
        }
    }
    fn extract_host_id(&self) -> Result<(String, String)> {
        // 匹配主机部分（非贪婪匹配直到 /#）
        let host_re = Regex::new(r"http://(.*?)/#")?;
        let host = host_re
            .captures(&self.url)
            .and_then(|cap| cap.get(1))
            .map(|m| m.as_str().to_string())
            .ok_or_else(|| anyhow!("URL 格式错误，无法提取主机"))?;

        // 匹配 ID 部分
        let id_re = Regex::new(r"id=([^&]*)")?;
        let id = id_re
            .captures(&self.url)
            .and_then(|cap| cap.get(1))
            .map(|m| m.as_str().to_string())
            .ok_or_else(|| anyhow!("URL 格式错误，无法提取 ID"))?;

        Ok((host, id))
    }

    fn head(&self) -> Result<(HeaderMap, String, String, String)> {
        let (host, id) = self.extract_host_id()?;

        // 构建 URL
        let url0 = format!("http://{}/WebDb/sql/parse", host);
        let url1 = format!(
            "http://{}/WebDb/operation/exe?id={}&db={}&actuator=editor-actuator&sql=&jk=true",
            host, id, self.ku
        );
        let url2 = format!("http://{}/WebDb/operation/exe?id={}&db={}&actuator=editor-actuator&sql=&jk=&length=1000", host, id, self.ku);

        // 构建请求头
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/json, text/plain, */*"),
        );
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip, deflate"));
        headers.insert(
            ACCEPT_LANGUAGE,
            HeaderValue::from_static("zh-CN,zh;q=0.9,ja;q=0.8"),
        );
        headers.insert(CONNECTION, HeaderValue::from_static("keep-alive"));
        headers.insert(ORIGIN, HeaderValue::from_str(&format!("http://{}", host))?);
        headers.insert(
            REFERER,
            HeaderValue::from_str(&format!("http://{}/", host))?,
        );
        headers.insert(
            USER_AGENT,
            HeaderValue::from_static("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"),
        );

        Ok((headers, url0, url1, url2))
    }

    async fn get_data(&self, sql: &str) -> Result<Vec<HashMap<String, Value>>> {
        let (headers, url0, url1, url2) = self.head()?;
        let client = reqwest::Client::new();

        // 第一个请求 (url0)
        let data0 = json!({"sql": sql.replace("\n"," "), "type": "sql"});
        let _res0 = client
            .post(&url0)
            .headers(headers.clone())
            .json(&data0)
            .send()
            .await
            .context("URL0 请求失败")?;

        // 第二个请求 (url1)
        let data = json!({"sql": sql.replace("\n"," ")});
        let _res1 = client
            .post(&url1)
            .headers(headers.clone())
            .json(&data)
            .send()
            .await
            .context("URL1 请求失败")?;

        // 第三个请求 (url2)
        let res2 = client
            .post(&url2)
            .headers(headers)
            .json(&data)
            .send()
            .await
            .context("URL2 请求失败")?;

        // 解析响应
        let response_text = res2.text().await.context("无法获取响应文本")?;
        println!("--{}", response_text);

        let json_response: Value =
            serde_json::from_str(&response_text).context("无法解析JSON响应")?;

        // 按原Python逻辑提取数据
        let result = json_response["data"]
            .as_array()
            .ok_or_else(|| anyhow!("响应中缺少 'data' 字段或不是数组"))?
            .get(0)
            .ok_or_else(|| anyhow!("'data' 数组为空"))?
            .get("data")
            .ok_or_else(|| anyhow!("缺少二级 'data' 字段"))?
            .get("data")
            .ok_or_else(|| anyhow!("缺少三级 'data' 字段"))?
            .as_array()
            .ok_or_else(|| anyhow!("'data' 字段不是数组"))?
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
}

pub struct DealBat {
    juhe_url: String,
    user_id_str: String,
    today: String,
    type1: String,
}
impl DealBat {
    pub fn new(juhe_url: String, user_id_str: String, today: String, type1: String) -> Self {
        DealBat {
            juhe_url,
            user_id_str,
            today,
            type1,
        }
    }

    fn generate_id(&self) -> String {
        let mut rng1 = rand::rng();
        let random_str: String = (0..10)
            .map(|_| {
                // 生成 0-9 的随机数字并转换为字符
                rng1.random_range(b'0'..=b'9') as char
            })
            .collect();
        format!("{}{}", self.today, random_str)
    }

    fn split_json_to_data_fields(&self, json_str: String) -> String {
        let data_field = [17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 500];
        // 转换为字符向量，确保安全处理多字节字符
        let chars: Vec<char> = json_str
            .replace("\\", "")
            .trim_matches('"')
            .chars()
            .collect();
        let mut current_idx = 0;
        let mut result_string = String::new();
        for &field_len in data_field.iter() {
            // 检查剩余字符是否足够
            if current_idx >= chars.len() {
                // 如果还有未处理的字段但已无内容，直接返回
                break;
            }
            // 计算实际分割的结束位置
            let end = std::cmp::min(current_idx + field_len, chars.len());

            // 提取字符片段并转换为字符串
            let segment = chars[current_idx..end].iter().collect::<String>();

            // 添加到结果字符串（如果不是第一个片段则添加逗号）
            if !result_string.is_empty() {
                result_string.push(',');
            }
            let segment2 = format!("'{}'", &segment);
            result_string.push_str(&segment2);

            // 更新当前索引位置
            current_idx = end;
        }

        // 如果字符串没有完全处理完，添加剩余部分
        if current_idx < chars.len() {
            let remaining = chars[current_idx..].iter().collect::<String>();
            if !result_string.is_empty() {
                result_string.push(',');
            }
            result_string.push_str(&remaining);
        }

        result_string
    }

    async fn get_num(&self) -> Result<Vec<HashMap<String, Value>>, Box<dyn Error>> {
        let sql = format!(
            "SELECT DISTINCT
    a.USER_ID,
    CONCAT(
        '{{\"businessType\":\"1\",\"custLongId\":', a.PARTITION_ID,
        ',\"customerName\":\"', COALESCE(b.attr_value, ''),
        '\",\"productId\":\"', a.product_id,
        '\",\"tradeCustomerType\":\"2\",\"tradeStaffId\":\"SUPERUSR\",\"userId\":', a.user_id, '}}'
    ) AS 'DATA'
FROM crm.tf_f_user_product a
LEFT JOIN crm.tf_F_user_attr b
    ON a.user_id = b.user_id
    AND b.ATTR_CODE = '0000000019'
WHERE a.user_id in ({});",
            &self.user_id_str
        );

        let app = Apps::new(&self.juhe_url, "crm");
        let data = app.get_data(&sql).await?;
        println!("--1{:?}", data);
        Ok(data)
    }

    pub async fn makesql(&self) -> Result<Vec<HashMap<String,String>>, Box<dyn Error>> {
        let sum_list = self.get_num().await;
        let month = &self.today[4..6];
        let date2 = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        match sum_list {
            Ok(data) => {
                let result: Vec<String> = data
                    .iter()
                    .map(|map| {
                        let batch_task_id  =&self.generate_id();
                        let batch_id  =&self.generate_id();
                        let operate_id  =&self.generate_id();
                        let user_id = &map["USER_ID"];
                        let value1 =  &map["DATA"];
                        let datax = &self.split_json_to_data_fields(value1.to_string());
                         format!("('{0}', '{1}', '{2}', '{3}', '{4}', {5}, '{6}', '{7}', {8}, {9}, {10}, {11}, {12}, '{13}'),",
                                                batch_task_id,batch_id,month,operate_id,&self.type1,100,date2,date2,user_id,250,datax,0,0,date2
                                        )

                                    }).collect();



                let result2:Vec<String>=result.iter().map(|x| {
                            format!("('{0}', '{1}', '{2}', '{3}', '{4}', {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}),",
                                    x[2..20].to_string(), x[24..42].to_string(), x[46..48].to_string(), &self.type1, date2, "'SUPERUSR'", "'00000'", "'250'", "'0'", "'0'", "'0'", "'1'", "'0'")
                        }).collect();
                let resx3 = format!(
                    "select * from tf_b_trade_2025 where ACCEPT_DATE > '{0}' and user_id in ({1});",
                    date2, &self.user_id_str
                );
              let mut resx: Vec<HashMap<String, String>> = Vec::from([
                    HashMap::from([
                        ("type2".to_string(), self.type1.to_string()),
                        ("bat1".to_string(), "INSERT INTO tf_b_trade_batdeal (BATCH_TASK_ID, BATCH_ID, ACCEPT_MONTH, OPERATE_ID, BATCH_OPER_TYPE, PRIORITY, REFER_TIME, EXEC_TIME, user_id, ROUTE_EPARCHY_CODE, DATA1, DATA2, DATA3, DATA4, DATA5, DATA6, DATA7, DATA8, DATA9, DATA10, DATA11, CANCEL_TAG, DEAL_STATE, DEAL_TIME) VALUES".to_string()),
                        ("bat2".to_string(), "INSERT INTO tf_b_trade_bat (BATCH_TASK_ID, BATCH_ID, ACCEPT_MONTH, BATCH_OPER_TYPE, ACCEPT_DATE, STAFF_ID, DEPART_ID, EPARCHY_CODE, IN_MODE_CODE, BATCH_COUNT, REMOVE_TAG, ACTIVE_FLAG, AUDIT_STATE) VALUES".to_string()),
                        ("check1".to_string(), resx3.clone()),
                    ])
                ]);

                for i in 0..result.len() {
                    let data1 = result[i].clone();
                    let data2 = result2[i].clone();
                    let datasx = HashMap::from([
                        ("type2".to_string(),self.type1.to_string()),
                        ("bat1".to_string(),data1),
                        ("bat2".to_string(),data2),
                        ("check1".to_string(),resx3.clone()),
                    ]);
                    resx.push(datasx);
                }
                println!("resx    {:#?}", resx);
                Ok(resx)
            }
            Err(e) => Err(e),
        }
    }
}
