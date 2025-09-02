<template>
  <div class="layout">
    <div class="sidebar">
      <el-row :gutter="0" class="tac">
        <el-col :span="24">
          <h2 class="el-text title-center">三线运维工具</h2>
          <el-menu
              background-color="#f0faff"
              default-active="2"
              class="el-menu-vertical-demo"
              @open="handleOpen"
              @close="handleClose"
          >
            <el-sub-menu index="1">
              <template #title>
                <el-icon><icon-menu /></el-icon>
                <span>待开发工具</span>
              </template>
              <el-menu-item-group title="Group One">
                <el-menu-item index="1-1">item one</el-menu-item>
                <el-menu-item index="1-2">item two</el-menu-item>
              </el-menu-item-group>
              <el-menu-item-group title="Group Two">
                <el-menu-item index="1-3">item three</el-menu-item>
              </el-menu-item-group>
              <el-sub-menu index="1-4">
                <template #title>item four</template>
                <el-menu-item index="1-4-1">item one</el-menu-item>
              </el-sub-menu>
            </el-sub-menu>
            <el-menu-item index="2" @click="goPage('shixiao')">
              <el-icon><Watermelon /></el-icon>
              <span>生成失效资源</span>
            </el-menu-item>
            <el-menu-item index="3" @click="goPage('qingfen')">
              <el-icon><Pear /></el-icon>
              <span>生成同步清分</span>
            </el-menu-item>
            <el-menu-item index="4" @click="goPage('page2')">
              <el-icon><Burger /></el-icon>
              <span>生成基础口令</span>
            </el-menu-item>
            <el-menu-item index="5" @click="goPage('page1')">
              <el-icon><Apple /></el-icon>
              <span>到期业务巡检</span>
            </el-menu-item>
            <el-menu-item index="6" disabled>
              <el-icon><Grape /></el-icon>
              <span>待添加</span>
            </el-menu-item>
            <el-menu-item index="7" disabled>
              <el-icon><document /></el-icon>
              <span>待添加</span>
            </el-menu-item>
            <el-menu-item index="8">
              <el-icon><setting /></el-icon>
              <span>设置</span>
            </el-menu-item>

          </el-menu>
        </el-col>
      </el-row>
    </div>
    <!-- 右侧内容区域（嵌套路由出口） -->
    <div class="content">
      <router-view></router-view>
    </div>
  </div>
</template>

<script setup lang="ts">
import { useRouter } from 'vue-router'
import {
  Document,
  Menu as IconMenu,
  Setting,
} from '@element-plus/icons-vue'

const handleOpen = (key:string, keyPath:string[]) => {
  console.log(key, keyPath)
}
const handleClose = (key:string, keyPath:string[]) => {
  console.log(key, keyPath)
}

const router = useRouter()
function goPage(page:string) {
  router.push({ path: page })
}
</script>

<style scoped>
/* 外层容器 */
.layout {
  display: flex;
  height: 100vh;
  padding: 0;
  margin: 0;
}

/* 左侧区域 */
.sidebar {
  width: 200px;
  border-right: 1px solid #ccc;
  padding: 0;
  background-color: #f0faff;
}

/* 右侧区域 */
.content {
  flex: 1;
  padding: 24px;
}

.title-center {
  color: dimgray;
  text-align: center;
  font-size: var(--el-font-size-extra-large);
  margin: 0;
}
</style>

<style>
html, body, #app {
  margin: 0;
  padding: 0;
  height: 100%;
}
</style>
