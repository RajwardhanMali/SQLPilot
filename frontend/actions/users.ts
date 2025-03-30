import { getServerUser } from "@/lib/session";
import User from "@/models/User";

export const getUserProfile = async () => {
  const user = await getServerUser();
  if (!user) throw new Error("User not found");
  const dbUser = await User.findById(user._id).populate("connections");
  if (!dbUser) throw new Error("User not found");

  return JSON.stringify(dbUser);
};
