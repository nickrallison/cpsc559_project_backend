import React, { useState, useEffect } from "react";
import axios from "axios";
import { FaEdit, FaTrash } from "react-icons/fa";
import "../App.css";

// We'll assume REACT_APP_HTTPPORT is now something like "100.81.146.3:8080"
// and REACT_APP_BACKEND_URLS might be "100.81.146.3:8080,100.81.146.3:8081,100.81.146.3:8082"
const Home = () => {
  const [newTask, setNewTask] = useState("");
  const [tasks, setTasks] = useState([]);
  const [statusMsg, setStatusMsg] = useState("");

  // For editing tasks
  const [editingTask, setEditingTask] = useState(null);
  const [editedText, setEditedText] = useState("");

  // For delete confirmation
  const [deletingTask, setDeletingTask] = useState(null);

  // Current active backend address (tailscaleIP:port)
  const [backendAddress, setBackendAddress] = useState(
    process.env.REACT_APP_HTTPPORT
  );

  // Update the active backend address
  const setHTTP = (address) => {
    setBackendAddress(address);
  };

  // Construct the base URL using the active backend address
  const baseURL = `http://${backendAddress}`;

  // Fetch tasks (assuming userId=1)
  const fetchTasks = () => {
    axios
      .get(`${baseURL}/objects?userId=1`)
      .then((response) => {
        setTasks(response.data);
        setStatusMsg("");
      })
      .catch((error) => {
        setStatusMsg("Error fetching tasks");
        console.error("Fetch error:", error);
        handleBackendError();
      });
  };

  // Add a new task
  const addTask = (e) => {
    e.preventDefault();
    if (!newTask.trim()) return;
    const taskObject = {
      user_id: 1,
      user_message_id: Date.now(),
      data: newTask,
      completed: false, // new tasks default to false
    };
    axios
      .post(`${baseURL}/objects`, [taskObject])
      .then(() => {
        // setStatusMsg("Task added successfully!");
        setNewTask("");
        fetchTasks();
      })
      .catch((error) => {
        setStatusMsg("Error adding task");
        console.error("Error:", error);
        handleBackendError();
      });
  };

  // Toggle completion status
  const toggleTaskCompletion = (task) => {
    const marker = " [Done]";
    let updatedData;
    if (task.data.includes(marker)) {
      // Remove the first occurrence of the marker
     
      updatedData = task.data.replace(marker, "").trim();
    } else {
      updatedData = task.data + marker;
    }
    const updatedTask = { ...task, data: updatedData };
    axios
      .put(`${baseURL}/objects`, updatedTask)
      .then(() => {
        fetchTasks();
      })
      .catch((error) => {
        setStatusMsg("Error updating task");
        console.error("Update error:", error);
        handleBackendError();
      });
  };

  // Open the edit popup
  const openEditPopup = (task) => {
    setEditingTask(task);
    setEditedText(task.data);
  };

  // Close the edit popup
  const closeEditPopup = () => {
    setEditingTask(null);
    setEditedText("");
  };

  // Submit the edited task
  const submitEdit = () => {
    if (editedText.trim() === "") return;
    const updatedTask = { ...editingTask, data: editedText };
    axios
      .put(`${baseURL}/objects`, updatedTask)
      .then(() => {
        setStatusMsg("Task updated successfully!");
        fetchTasks();
        closeEditPopup();
      })
      .catch((error) => {
        setStatusMsg("Error updating task");
        console.error("Update error:", error);
        handleBackendError();
      });
  };

  // Open delete confirmation popup
  const openDeletePopup = (task) => {
    setDeletingTask(task);
  };

  // Close delete confirmation popup
  const closeDeletePopup = () => {
    setDeletingTask(null);
  };

  // Confirm delete action
  const confirmDelete = () => {
    axios
      .delete(
        `${baseURL}/objects?userId=${deletingTask.user_id}&userMessageId=${deletingTask.user_message_id}`
      )
      .then(() => {
        setStatusMsg("Task deleted successfully!");
        fetchTasks();
        closeDeletePopup();
      })
      .catch((error) => {
        setStatusMsg("Error deleting task");
        console.error("Delete error:", error);
        handleBackendError();
      });
  };

  // Check for an available backend from the list in REACT_APP_BACKEND_URLS (each is tailscaleIP:port).
  const handleBackendError = async () => {
    const addresses = (process.env.REACT_APP_BACKEND_URLS || "").split(",");
    for (let addr of addresses) {
      const trimmedAddr = addr.trim();
      if (!trimmedAddr) continue;
      try {
        const response = await axios.get(`http://${trimmedAddr}/alive`);
        if (response.data === "alive") {
          setHTTP(trimmedAddr);
          console.log(`Switched backend to: ${trimmedAddr}`);
          break;
        }
      } catch (err) {
        console.error(`Backend address ${trimmedAddr} not responding.`);
      }
    }
  };

  // Poll tasks periodically
  useEffect(() => {
    const pollingInterval = setInterval(() => {
      fetchTasks();
    }, 1000);
    return () => clearInterval(pollingInterval);
  }, [backendAddress]); // Refetch tasks when backendAddress changes

  // Separate tasks into planned and completed
  const plannedTasks = tasks.filter((task) => !task.data.includes("[Done]"));
  const completedTasks = tasks.filter((task) => task.data.includes("[Done]"));

  return (
    <div className="container">
      <h1>Shared To‑Do List</h1>
      <form className="task-form" onSubmit={addTask}>
        <input
          type="text"
          placeholder="Enter new task..."
          value={newTask}
          onChange={(e) => setNewTask(e.target.value)}
          required
        />
        <button type="submit">Add Task</button>
      </form>
      {statusMsg && <p className="status-msg">{statusMsg}</p>}

      {/* Planned Tasks */}
      <h2>Planned</h2>
      <ul className="task-list">
        {plannedTasks.map((task) => (
          <li
            key={task.user_message_id}
            className={task.data.includes("[Done]") ? "completed-task" : ""}
          >
            {/* Left side: checkbox + text */}
            <div className="task-left">
              <input
                type="checkbox"
                checked={task.data.includes("[Done]")}
                onChange={() => toggleTaskCompletion(task)}
              />
              <span className="task-text">{task.data}</span>
            </div>

            {/* Right side: edit/delete icons */}
            <div className="actions">
              <button
                onClick={() => openEditPopup(task)}
                className="icon-button edit-icon"
              >
                <FaEdit />
              </button>
              <button
                onClick={() => openDeletePopup(task)}
                className="icon-button delete-icon"
              >
                <FaTrash />
              </button>
            </div>
          </li>
        ))}
      </ul>

      {/* Completed Tasks */}
      <h2>Completed</h2>
      <ul className="task-list">
        {completedTasks.map((task) => (
          <li
            key={task.user_message_id}
            className={task.data.includes("[Done]") ? "completed-task" : ""}
          >
            <div className="task-left">
              <input
                type="checkbox"
                checked={task.data.includes("[Done]")}
                onChange={() => toggleTaskCompletion(task)}
              />
              <span className="task-text">{task.data}</span>
            </div>

            <div className="actions">
              <button
                onClick={() => openEditPopup(task)}
                className="icon-button edit-icon"
              >
                <FaEdit />
              </button>
              <button
                onClick={() => openDeletePopup(task)}
                className="icon-button delete-icon"
              >
                <FaTrash />
              </button>
            </div>
          </li>
        ))}
      </ul>

      {/* Modal Popup for Editing Task */}
      {editingTask && (
        <div className="modal-overlay">
          <div className="modal">
            <h3>Edit Task</h3>
            <input
              type="text"
              value={editedText}
              onChange={(e) => setEditedText(e.target.value)}
            />
            <div className="modal-actions">
              <button onClick={submitEdit}>Save</button>
              <button onClick={closeEditPopup}>Cancel</button>
            </div>
          </div>
        </div>
      )}

      {/* Modal Popup for Delete Confirmation */}
      {deletingTask && (
        <div className="modal-overlay">
          <div className="modal delete-modal">
            <h3>Confirm Delete</h3>
            <p>Are you sure you want to delete this task?</p>
            <div className="modal-actions">
              <button onClick={confirmDelete}>Delete</button>
              <button onClick={closeDeletePopup}>Cancel</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default Home;
