"""TensorFlow/Keras DQN with experience replay and target network."""
from __future__ import annotations

import json
import random
from collections import deque
from pathlib import Path
from typing import Deque, List, Optional, Tuple

import numpy as np

from AI_Control_Traffic_Light.config.settings import RLConfig

try:
    import tensorflow as tf
    from tensorflow import keras
except ImportError as e:
    raise ImportError(
        "TensorFlow is required for DQN. Install: pip install -r AI_Control_Traffic_Light/requirements.txt"
    ) from e


class DQNAgent:
    def __init__(
        self,
        agent_id: str,
        config: RLConfig,
        *,
        state_dim: Optional[int] = None,
        trainable: bool = True,
    ) -> None:
        self.agent_id = agent_id
        self.config = config
        self.state_dim = state_dim or config.state_dim
        self.action_size = config.action_size
        self.epsilon = config.epsilon_start
        self.trainable = trainable
        self.memory: Deque[Tuple[np.ndarray, int, float, np.ndarray, bool]] = deque(
            maxlen=config.memory_size
        )
        self.online = self._build_model()
        self.target = self._build_model()
        self.target.set_weights(self.online.get_weights())
        self.optimizer = keras.optimizers.Adam(learning_rate=config.learning_rate)

    def _build_model(self) -> keras.Model:
        layers: List[keras.layers.Layer] = [keras.layers.Input(shape=(self.state_dim,))]
        for units in self.config.hidden_layers:
            layers.append(keras.layers.Dense(units, activation="relu"))
        layers.append(keras.layers.Dense(self.action_size, activation="linear"))
        x = layers[0]
        for layer in layers[1:]:
            x = layer(x)
        return keras.Model(inputs=layers[0], outputs=x)

    def act(self, state: np.ndarray, *, greedy: bool = False) -> int:
        if not greedy and random.random() < self.epsilon:
            return random.randrange(self.action_size)
        q = self.online.predict(state.reshape(1, -1), verbose=0)[0]
        return int(np.argmax(q))

    def remember(
        self,
        state: np.ndarray,
        action: int,
        reward: float,
        next_state: np.ndarray,
        done: bool,
    ) -> None:
        self.memory.append((state, action, reward, next_state, done))

    def replay(self) -> Optional[float]:
        if not self.trainable or len(self.memory) < self.config.batch_size:
            return None
        batch = random.sample(self.memory, self.config.batch_size)
        states = np.array([b[0] for b in batch], dtype=np.float32)
        actions = np.array([b[1] for b in batch], dtype=np.int32)
        rewards = np.array([b[2] for b in batch], dtype=np.float32)
        next_states = np.array([b[3] for b in batch], dtype=np.float32)
        dones = np.array([b[4] for b in batch], dtype=np.float32)

        next_q = self.target.predict(next_states, verbose=0)
        max_next = np.max(next_q, axis=1)
        targets = rewards + (1.0 - dones) * self.config.gamma * max_next

        with tf.GradientTape() as tape:
            q_values = self.online(states, training=True)
            idx = tf.range(self.config.batch_size, dtype=tf.int32)
            action_q = tf.gather(q_values, idx, batch_dims=1)
            action_q = tf.gather(action_q, actions, axis=1, batch_dims=1)
            action_q = tf.squeeze(action_q, axis=1)
            loss = tf.reduce_mean(keras.losses.Huber()(targets, action_q))

        grads = tape.gradient(loss, self.online.trainable_variables)
        self.optimizer.apply_gradients(zip(grads, self.online.trainable_variables))
        return float(loss.numpy())

    def update_target(self) -> None:
        self.target.set_weights(self.online.get_weights())

    def decay_epsilon(self) -> None:
        self.epsilon = max(self.config.epsilon_min, self.epsilon * self.config.epsilon_decay)

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.online.save(path)
        meta = {
            "agent_id": self.agent_id,
            "epsilon": self.epsilon,
            "state_dim": self.state_dim,
        }
        path.with_suffix(".meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    def load(self, path: Path) -> None:
        self.online = keras.models.load_model(path)
        self.target.set_weights(self.online.get_weights())
        meta_path = path.with_suffix(".meta.json")
        if meta_path.is_file():
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            self.epsilon = float(meta.get("epsilon", self.config.epsilon_min))
