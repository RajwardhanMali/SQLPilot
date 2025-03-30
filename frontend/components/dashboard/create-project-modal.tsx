"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import useDashboardStore from "@/store/dashboard";
import { createProject } from "@/actions/projects";

export default function CreateProjectModal({ open, setOpen }: any) {
  const router = useRouter();
  const { setProject } = useDashboardStore();
  const [isLoading, setIsLoading] = useState(false);

  const handleCreateProject = async (title: string, description: string) => {
    setIsLoading(true);
    try {
      const project = await createProject({ title, description });
      setProject(project);
      setOpen(false);
      router.push(`/dashboard/${project._id}`);
    } catch (error) {
      console.error("Error creating project:", error);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogContent className="bg-dark-100/95 backdrop-blur-md border border-white/10 rounded-lg">
        <DialogHeader>
          <DialogTitle>Create a Project</DialogTitle>
        </DialogHeader>
        <div className="space-y-4">
          <input
            type="text"
            required
            placeholder="Project Title"
            id="project-title"
            className="w-full px-4 py-2 bg-dark-200/80 border border-white/10 rounded-md text-white"
          />
          <textarea
            placeholder="Project Description"
            id="project-description"
            className="w-full px-4 py-2 bg-dark-200/80 border border-white/10 rounded-md text-white"
          ></textarea>
          <Button
            disabled={isLoading}
            onClick={() =>
              handleCreateProject(
                (document.getElementById("project-title") as HTMLInputElement)
                  .value,
                (
                  document.getElementById(
                    "project-description"
                  ) as HTMLTextAreaElement
                ).value
              )
            }
            className="w-full neon-gradient-bg text-white"
          >
            Create Project
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}
