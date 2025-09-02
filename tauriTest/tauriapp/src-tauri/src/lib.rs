pub mod jichu;
pub mod auto;
pub mod qingfen;
pub mod shixiao;

use std::collections::HashMap;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Deserialize, Serialize)]
struct Jichu {
    juhe1: String,
    user_id: String,
    type1: i32,
}


#[tauri::command]
async fn jichu1(jsonStr: &str) -> Result<Vec<HashMap<String,String>>, String> {
    let jichu22: Jichu =
        serde_json::from_str(jsonStr).map_err(|err| format!("JSON解析错误: {}", err))?;
    let date = Utc::now().format("%Y%m%d").to_string();
    let juhe_url = jichu22.juhe1;
    let user_id_str1 = jichu22.user_id;
    let bb: Vec<&str> = user_id_str1.split_whitespace().collect();
    let user_id_str = bb.join(",");
    let id1 = jichu22.type1.to_string();
    let data1: Value = json!({
        "1": "BACK_BASIC",
        "2": "STOP_BASIC",
        "3": "CANCEL_BASIC"
    });
    let type1 = data1
        .get(id1)
        .and_then(|v| v.as_str()) // 尝试转换为字符串
        .unwrap_or("");

    let counts = jichu::DealBat::new(juhe_url, user_id_str, date, type1.to_string());
    let content = counts.makesql().await.map_err(|e| e.to_string())?;

    Ok(content)
}


#[derive(Debug, Deserialize, Serialize)]
struct autoJson {
    jour2:String,
    juhe2: String,
    startdate: String,
    enddate: String,
}


#[tauri::command]
async fn libauto(jsonStr: &str) -> Result<(HashMap<String,String>,Vec<HashMap<String,Value>>), String> {
    let auto11: autoJson =
        serde_json::from_str(jsonStr).map_err(|err| format!("JSON解析错误: {}", err))?;

    let start_date = auto11.startdate.as_str();
    let end_date = auto11.enddate.as_str();
    let jour_url = auto11.jour2.as_str();
    let juhe_url = auto11.juhe2.as_str();

    let mut counts=auto::GetData::new(start_date, end_date, jour_url,juhe_url);
    let content2=counts.get_data().await.map_err(|e| e.to_string())?;
    println!("{:?}", content2);
    Ok(content2)
}

#[derive(Debug, Deserialize, Serialize)]
struct qingfenJson {
    jour1:String,
    trades: String,
}
#[tauri::command]
async fn libqingfen(jsonStr: &str) -> Result<Vec<HashMap<String,String>>, String> {
    let qingfen11: qingfenJson =
        serde_json::from_str(jsonStr).map_err(|err| format!("JSON解析错误: {}", err))?;

    let jour_url1 = qingfen11.jour1.as_str();
    let trade_str1 = qingfen11.trades.as_str();

    let counts=qingfen::Apps::new(jour_url1, trade_str1);
    let content2=counts.head().map_err(|e| e.to_string())?.get_index_info().await.map_err(|e| e.to_string())?;
    println!("{:?}", content2);
    Ok(content2)
}


#[derive(Debug, Deserialize, Serialize)]
struct shixiaoJson {
    crm1:String,
    user1: String,
}
#[tauri::command]
async fn libshixiao(jsonStr: &str) -> Result<Vec<HashMap<String,String>>, String> {
    let shixiao11: shixiaoJson =
        serde_json::from_str(jsonStr).map_err(|err| format!("JSON解析错误: {}", err))?;

    let crm_url1 = shixiao11.crm1;
    let users1 = shixiao11.user1;
    let bb: Vec<&str> = users1.split_whitespace().collect();
    let users = bb.join(",");

    let counts=shixiao::Apps::new(crm_url1.as_str(), users.as_str());
    let content2=counts.head().map_err(|e| e.to_string())?.get_crm_data().await.map_err(|e| e.to_string())?;
    println!("{:?}", content2);
    Ok(content2)
}


#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_fs::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![jichu1,libauto,libqingfen,libshixiao])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
