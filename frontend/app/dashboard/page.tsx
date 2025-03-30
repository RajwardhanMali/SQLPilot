"use client";

import { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import useDashboardStore from "@/store/dashboard";
import { getProjectsByUser } from "@/actions/projects";
import CreateProjectModal from "@/components/dashboard/create-project-modal";
import Loading from "./loading";

export default function DashboardPage() {
  const router = useRouter();
  const { setProject } = useDashboardStore();
  const [showProjectDialog, setShowProjectDialog] = useState(false);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    const fetchProjects = async () => {
      try {
        const projects = JSON.parse(await getProjectsByUser(1));
        if (projects.length > 0) {
          setProject(projects[0]);
          router.push(`/dashboard/${projects[0]._id}`);
        } else {
          setShowProjectDialog(true);
        }
      } catch (error) {
        console.error("Error fetching projects:", error);
        router.push("/");
      } finally {
        setIsLoading(false);
      }
    };

    fetchProjects();
  }, [router, setProject]);

  if (isLoading) {
    return <Loading />;
  }

  return (
    <CreateProjectModal
      open={showProjectDialog}
      setOpen={setShowProjectDialog}
    />
  );
}
