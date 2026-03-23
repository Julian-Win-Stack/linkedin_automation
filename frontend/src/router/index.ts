import { createRouter, createWebHistory } from "vue-router";
import ResearchView from "../views/ResearchView.vue";

export default createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: "/",
      name: "research",
      component: ResearchView,
    },
  ],
});
