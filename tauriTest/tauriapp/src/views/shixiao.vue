<template>
  <el-text style="display: inline-block; margin: 10px 0; margin-left: 25px;"  type="info">生成失效资源sql页面</el-text>
  <div>
    <el-form :model="Formshixiao" label-width="120px" :rules="formRuleshixiao">
      <el-row :gutter="20">
        <el-col :span="24">
          <el-form-item label="聚合库地址" prop="crm1">
            <el-input  v-model="Formshixiao.crm1" placeholder="请输入crm聚合库地址：http://"  />
          </el-form-item>
        </el-col>
      </el-row>

      <el-row :gutter="20">
        <el-col :span="24">
          <el-form-item label="输入user_id" prop="users">
            <el-input v-model="Formshixiao.user1" placeholder="请输入user_id,批量数据用空格隔开" />
          </el-form-item>
        </el-col>
      </el-row>
      <el-row :gutter="20">
        <el-col :span="14">
          <img src="../assets/222.png" alt="填充图片" title="填充图片" style="margin-left: 30px;" width="440" height="60">
        </el-col>
        <el-col :span="10">
          <el-button type="primary"  class="kuang" @click="submitshixiao" :loading="loading" :disabled="loading">生成执行sql</el-button>
          <el-button type="primary"  class="kuang" @click="exportTableToCSV">导出为CSV</el-button>
        </el-col>
      </el-row>
    </el-form>
    <div v-loading="loading" element-loading-text="生成中，请稍等...">
      <el-table-v2
          :columns="columns"
          :data="tableData"
          :width="720"
          :height="440"
          fixed
          class="biao"
      />
    </div>
    <el-row class="w-150px mb-2">
      <el-text truncated>温馨提示：</el-text>
    </el-row>
    <el-text line-clamp="3">
      1、该功根据输入的userId,生成手动失效资源的sql<br />
      2、根据生成的inser语句执行，需打开聚合库的crm页面<br />
    </el-text>

  </div>
</template>


<script setup lang="ts">
import { reactive,ref } from "vue";
import { invoke } from "@tauri-apps/api/core";
import { writeTextFile,BaseDirectory } from '@tauri-apps/plugin-fs';

const Formshixiao = reactive({crm1: "", user1: ""});
const loading = ref(false);
let tableData = ref<Shi[]>([]);

const headerLabelMap: Record<string, string> = {
  "失效user表":"sql_user",
  "失效product表":"sql_product",
  "失效discnt表":"sql_discnt",
  "失效话单表":"sql_bill",
  "失效oporderquery表":"sql_op",
}

interface Shi {
  sql_user:String,
  sql_product:String,
  sql_discnt :String,
  sql_bill:String,
  sql_op:String,
}
// 表单验证规则
const formRuleshixiao = reactive({

  crm1: [
    { required: true, message: '请输入crm聚合库网页地址', trigger: 'blur' }
  ],
  user1: [
    { required: true, message: '请输入修复的user_id', trigger: 'blur' }
  ],

});

const columns = Object.keys(headerLabelMap).map(key => ({
  key: headerLabelMap[key],
  dataKey: headerLabelMap[key],
  title: `${key}`,
  width: 160
}));
const generateData = (data:Shi[] ) =>
    data.map((item ,index)=> ({
      ...item,
      id: `row-${index}`,
      parentId: null,
    })) as (Shi & { id: string ,parentId:null})[];

const submitshixiao = async () => {
  try {
    loading.value = true
    console.log(Formshixiao);
    let res: Shi[]  = await invoke("libshixiao", { jsonStr: JSON.stringify(Formshixiao) });
    console.log(res);

    tableData.value = generateData(res);
  } catch (error) {
    console.error("获取实例数据出错:", error);
  }finally {loading.value = false}}
//转译csv字段
function escapeCSV(value: string): string {
  if (value == null) return '';
  const needsEscape = /[",\n]/.test(value);
  const escaped = value.replace(/"/g, '""'); // 转义双引号
  return needsEscape ? `"${escaped}"` : escaped;
}
const exportTableToCSV = async () => {

  const headers =["失效user表", "失效product表","失效discnt表","失效话单表","失效oporderquery表"];

  // 转换每一行数据
  const rows = tableData.value.map(row => {
    return headers.map(header => escapeCSV(String(row[headerLabelMap[header] as keyof Shi]))).join(',');
  });


  // 构建带 BOM 的 CSV 内容（确保 Excel 支持中文）
  const bom = '\uFEFF';
  const csvContent = bom + [headers.join(','), ...rows].join('\n');

  const now = new Date();
  const textpath =`失效资源执行sql${now.getFullYear().toString()}-${now.getMonth()+1}-${now.getDate()}.csv`;
  // 写入到 Downloads 文件夹
  await writeTextFile(textpath, csvContent, {
    baseDir: BaseDirectory.Download
  });

  alert(`文件已保存到你的下载目录，请注意查看`);
}


</script>

<style scoped>
.kuang{
  margin-left: 40px;
}

.biao {
  margin-top: 20px;
  overflow: hidden;
}
</style>