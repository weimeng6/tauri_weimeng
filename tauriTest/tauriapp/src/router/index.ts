import { createRouter, createWebHistory } from 'vue-router'
import MainPage from '../views/MainPage.vue'
import Page1 from '../views/Page1.vue'
import Page2 from '../views/Page2.vue'
import Qingfen1 from '../views/qingfen.vue'
import Shixiao1 from '../views/shixiao.vue'

const routes = [
    {
        path: '/',
        component: MainPage,
        children: [
            { path: '', redirect: 'shixiao' },
            { path: 'shixiao', component: Shixiao1 },
            { path: 'qingfen', component: Qingfen1 },
            { path: 'page1', component: Page1 },
            { path: 'page2', component: Page2 }

        ]
    }
]
const router = createRouter({
    history: createWebHistory(),
    routes,
})

export default router