import { defineConfig } from "vocs";

export default defineConfig({
  rootDir: "./src",
  title: "BullMQ for Golang",
  description:
    "A Golang port of BullMQ, a Node.js library for handling distributed jobs and messages in Node.js applications with Redis.",
  editLink: {
    pattern:
      "https://github.com/codycody31/gobullmq/edit/main/docs/src/pages/:path",
    text: "Edit on GitHub",
  },
  sidebar: [
    {
      text: "Overview",
      items: [
        { text: "Introduction", link: "/introduction" },
        { text: "Getting Started", link: "/getting-started" },
      ],
    },
    {
      text: "Queue Managment",
      link: "/queue-management",
    },
    {
      text: "Worker Processing",
      link: "/worker-processing",
    },
    {
      text: "Event Handling",
      link: "/event-handling",
    },
    {
      text: "Customization",
      link: "/customization",
    },
    {
      text: "API",
      items: [
        {
          text: "Job",
          items: [
            { text: "Options", link: "/api/job/options" },
            { text: "Repeatable", link: "/api/job/repeatable" },
          ],
        },
      ],
    },
    { text: "Credits & Upstream", link: "/credits" },
  ],
  socials: [
    {
      icon: "github",
      link: "https://github.com/codycody31/gobullmq",
    },
  ],
  markdown: {
    code: {
      themes: {
        light: "github-light",
        dark: "github-dark",
      },
    },
  },
});
