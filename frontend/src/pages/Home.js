import React, { useState, useEffect } from "react";
import axios from "axios";
import { FaEdit, FaTrash } from "react-icons/fa";
import "../App.css";

const HTTPPORT = process.env.REACT_APP_HTTPPORT;

const Home = () => {
  const [newTask, setNewTask] = useState("");
  const [tasks, setTasks] = useState([]);
  const [statusMsg, setStatusMsg] = useState("");

  // For editing tasks
  const [editingTask, setEditingTask] = useState(null);
  const [editedText, setEditedText] = useState("");

  // For delete confirmation
  const [deletingTask, setDeletingTask] = useState(null);

  // Fetch tasks (assuming userId=1)
  const fetchTasks = () => {
    axios
      .get(`http://${window.location.hostname}:${HTTPPORT}/objects?userId=1`)
      .then((response) => {
        setTasks(response.data);
        setStatusMsg("");
      })
      .catch((error) => {
        setStatusMsg("Error fetching tasks");
        console.error("Fetch error:", error);
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
      .post(`http://${window.location.hostname}:${HTTPPORT}/objects`, [
        taskObject,
      ])
      .then(() => {
        setStatusMsg("Task added successfully!");
        setNewTask("");
        fetchTasks();
      })
      .catch((error) => {
        setStatusMsg("Error adding task");
        console.error("Error:", error);
      });
  };

  // Toggle completion status
  const toggleTaskCompletion = (task) => {
    const marker = " [Done]";
    let updatedData;
    if (task.data.includes(marker)) {
      // Remove the first occurrence of the marker.
      // Using Replace with count 1 ensures that only the first instance is removed.
      updatedData = task.data.replace(marker, "").trim();
    } else {
      updatedData = task.data + marker;
    }
    const updatedTask = { ...task, data: updatedData };
    axios
      .put(
        `http://${window.location.hostname}:${HTTPPORT}/objects`,
        updatedTask
      )
      .then(() => {
        fetchTasks();
      })
      .catch((error) => {
        setStatusMsg("Error updating task");
        console.error("Update error:", error);
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
      .put(
        `http://${window.location.hostname}:${HTTPPORT}/objects`,
        updatedTask
      )
      .then(() => {
        setStatusMsg("Task updated successfully!");
        fetchTasks();
        closeEditPopup();
      })
      .catch((error) => {
        setStatusMsg("Error updating task");
        console.error("Update error:", error);
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
        `http://${window.location.hostname}:${HTTPPORT}/objects?userId=${deletingTask.user_id}&userMessageId=${deletingTask.user_message_id}`
      )
      .then(() => {
        setStatusMsg("Task deleted successfully!");
        fetchTasks();
        closeDeletePopup();
      })
      .catch((error) => {
        setStatusMsg("Error deleting task");
        console.error("Delete error:", error);
      });
  };

  // Poll tasks periodically
  useEffect(() => {
    const pollingInterval = setInterval(() => {
      fetchTasks();
    }, 1000);
    return () => clearInterval(pollingInterval);
  }, []);

  // Separate tasks into planned vs. completed
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
            <input
              type="checkbox"
              checked={task.data.includes("[Done]")}
              onChange={() => toggleTaskCompletion(task)}
            />
            <span className="task-text">{task.data}</span>
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
            <input
              type="checkbox"
              checked={task.data.includes("[Done]")}
              onChange={() => toggleTaskCompletion(task)}
            />
            <span className="task-text">{task.data}</span>
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
