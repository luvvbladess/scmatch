#Основа

import asyncio
import json
from typing import Any, Dict, List, Optional

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

from config import (
    AGE_DELTA,
    CANDIDATES_LIMIT,
    DB_PATH,
    BOT_TOKEN,
    logger,
)
from db import (
    get_profile,
    upsert_profile,
    record_interaction,
    has_interaction,
    count_pending_likers,
    get_next_pending_liker,
    init_db,
    get_virtual_state,
    set_virtual_state,
)
from ai_utils import get_text_embedding, virtual_reply, cosine_similarity

# =========================
# Вспомогательные функции
# =========================

def clamp_age(value: int) -> int:
    return max(18, min(99, value))

def is_profile_complete(p: Dict[str, Any]) -> bool:
    req = ["name", "age", "city", "gender", "description", "photo_file_id"]
    return all(p.get(k) for k in req)

def profile_caption(p: Dict[str, Any], include_username: bool = False) -> str:
    parts: List[str] = []
    parts.append(f"{p.get('name','Без имени')}, {p.get('age','?')}")
    parts.append(f"Город: {p.get('city','—')}")
    parts.append("")
    desc = (p.get("description") or "").strip()
    parts.append(desc)
    if include_username and p.get("username"):
        parts.append("")
        parts.append(f"@{p['username']}")
    return "\n".join(parts)

# =========================
# Клавиатуры
# =========================

def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Создать/Редактировать анкету")],
            [KeyboardButton(text="Редактировать анкету")],
            [KeyboardButton(text="Посмотреть мою анкету")],
            [KeyboardButton(text="Поиск анкет")],
            [KeyboardButton(text="Предпочтения поиска")],
            [KeyboardButton(text="Виртуальный собеседник")],
            [KeyboardButton(text="Помощь")],
        ],
        resize_keyboard=True,
    )

def gender_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Мужчина"), KeyboardButton(text="Женщина")],
            [KeyboardButton(text="Отмена")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def looking_for_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Ищу мужчин"), KeyboardButton(text="Ищу женщин")],
            [KeyboardButton(text="Ищу кого угодно")],
            [KeyboardButton(text="Отмена")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def virtual_partner_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Виртуальный мужчина"), KeyboardButton(text="Виртуальная женщина")],
            [KeyboardButton(text="Закончить виртуальный чат")],
            [KeyboardButton(text="Назад в меню")],
        ],
        resize_keyboard=True,
    )

def profile_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👍 Лайк", callback_data="like"),
             InlineKeyboardButton(text="👎 Дизлайк", callback_data="dislike")],
            [InlineKeyboardButton(text="🔎 Моя анкета", callback_data="my_profile")],
            [InlineKeyboardButton(text="⛔️ Стоп", callback_data="stop_search")],
        ]
    )

def likers_inline_kb(liker_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👍 Лайк", callback_data=f"liker_like:{liker_id}"),
             InlineKeyboardButton(text="👎 Дизлайк", callback_data=f"liker_dislike:{liker_id}")],
            [InlineKeyboardButton(text="⛔️ Стоп", callback_data="stop_likers")],
        ]
    )

def show_likers_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Показать лайкнувших", callback_data="show_likers")],
        ]
    )

def go_to_search_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Перейти к просмотру анкет", callback_data="go_to_search")],
        ]
    )

def edit_fields_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Имя"), KeyboardButton(text="Возраст")],
            [KeyboardButton(text="Город"), KeyboardButton(text="Пол")],
            [KeyboardButton(text="Кого ищу"), KeyboardButton(text="Описание")],
            [KeyboardButton(text="Фото")],
            [KeyboardButton(text="Готово")],
        ],
        resize_keyboard=True,
    )

# =========================
# FSM состояния
# =========================

class ProfileFSM(StatesGroup):
    name = State()
    age = State()
    city = State()
    gender = State()
    looking_for = State()
    description = State()
    photo = State()

class EditFSM(StatesGroup):
    field_choice = State()
    value_input = State()

class VirtualChatFSM(StatesGroup):
    choose_partner = State()
    chatting = State()

# =========================
# Поиск кандидатов (SQL + ранжирование)
# =========================

async def find_candidates(user_id: int, limit: int = CANDIDATES_LIMIT) -> List[Dict[str, Any]]:
    me = await get_profile(user_id)
    if not me or not is_profile_complete(me):
        return []

    my_gender = me["gender"]
    my_lf = me.get("looking_for") or "ANY"

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        params = [
            user_id,
            me["city"],
            my_lf,
            my_lf,
            my_gender,
            me["age"],
            AGE_DELTA,
            user_id,
            limit,
        ]
        sql = """
        SELECT * FROM profiles
        WHERE user_id != ?
          AND city = ?
          AND (
                ? = 'ANY' OR gender = ?
          )
          AND (
                looking_for = 'ANY' OR looking_for = ?
          )
          AND ABS(age - ?) <= ?
          AND name IS NOT NULL
          AND age IS NOT NULL
          AND city IS NOT NULL
          AND gender IS NOT NULL
          AND description IS NOT NULL
          AND photo_file_id IS NOT NULL
          AND user_id NOT IN (SELECT target_id FROM interactions WHERE user_id = ?)
        LIMIT ?
        """
        cur = await db.execute(sql, params)
        rows = await cur.fetchall()
    candidates = [dict(r) for r in rows]

    # Ранжирование по эмбеддингам описаний
    try:
        my_emb = None
        if me.get("embedding"):
            try:
                my_emb = json.loads(me["embedding"]) if isinstance(me["embedding"], str) else me["embedding"]
            except Exception:
                my_emb = None
        if my_emb is None:
            text = (me.get("description") or "").strip()
            if text:
                my_emb = await get_text_embedding(text)
                await upsert_profile(user_id, embedding=my_emb)
        if my_emb:
            with_scores = []
            no_emb = []
            for c in candidates:
                emb = None
                if c.get("embedding"):
                    try:
                        emb = json.loads(c["embedding"]) if isinstance(c["embedding"], str) else c["embedding"]
                    except Exception:
                        emb = None
                if emb:
                    score = cosine_similarity(my_emb, emb)
                    with_scores.append((score, c))
                else:
                    no_emb.append(c)
            with_scores.sort(key=lambda x: x[0], reverse=True)
            ranked = [c for _, c in with_scores] + no_emb
            return ranked
        else:
            return candidates
    except Exception as e:
        logger.exception(f"Ошибка ранжирования: {e}")
        return candidates

# =========================
# Бот и роутеры
# =========================

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# =========================
# Хэндлеры
# =========================

@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await init_db()
    await state.clear()
    await message.answer(
        "Привет! Это бот знакомств.\n"
        "Создайте анкету и начинайте знакомиться.\n"
        "Возрастной поиск: ±2 года.\n"
        "Меню ниже.",
        reply_markup=main_menu(),
    )

@dp.message(F.text == "Помощь")
async def help_msg(message: Message):
    await message.answer(
        "Что я умею:\n"
        "- Создать/редактировать анкету (мастер)\n"
        "- Редактировать анкету (по полям)\n"
        "- Посмотреть мою анкету\n"
        "- Поиск анкет (лайк/дизлайк, совпадения)\n"
        "- Предпочтения поиска (пол/кого искать)\n"
        "- Виртуальный собеседник (М/Ж)\n\n"
        "Поиск учитывает город, взаимные предпочтения по полу и возраст ±2 года.\n"
        "Похожесть описаний — через эмбеддинги."
    )

# -------- Анкета: мастер создания --------

@dp.message(F.text == "Создать/Редактировать анкету")
async def create_or_edit_profile(message: Message, state: FSMContext):
    await state.set_state(ProfileFSM.name)
    await message.answer(
        "Введите ваше имя:",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Отмена")]], resize_keyboard=True),
    )

@dp.message(ProfileFSM.name, F.text.casefold() == "отмена")
@dp.message(ProfileFSM.age, F.text.casefold() == "отмена")
@dp.message(ProfileFSM.city, F.text.casefold() == "отмена")
@dp.message(ProfileFSM.gender, F.text.casefold() == "отмена")
@dp.message(ProfileFSM.looking_for, F.text.casefold() == "отмена")
@dp.message(ProfileFSM.description, F.text.casefold() == "отмена")
@dp.message(ProfileFSM.photo, F.text.casefold() == "отмена")
async def cancel_profile(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Отменено.", reply_markup=main_menu())

@dp.message(ProfileFSM.name)
async def fsm_name(message: Message, state: FSMContext):
    name = (message.text or "").strip()
    if not name:
        await message.answer("Имя не может быть пустым. Введите имя:")
        return
    await state.update_data(name=name)
    await state.set_state(ProfileFSM.age)
    await message.answer("Введите ваш возраст (18-99):")

@dp.message(ProfileFSM.age)
async def fsm_age(message: Message, state: FSMContext):
    try:
        age = int((message.text or "").strip())
        age = clamp_age(age)
    except Exception:
        await message.answer("Введите число от 18 до 99:")
        return
    await state.update_data(age=age)
    await state.set_state(ProfileFSM.city)
    await message.answer("Введите ваш город:")

@dp.message(ProfileFSM.city)
async def fsm_city(message: Message, state: FSMContext):
    city = (message.text or "").strip()
    if not city:
        await message.answer("Город не может быть пустым. Введите город:")
        return
    await state.update_data(city=city)
    await state.set_state(ProfileFSM.gender)
    await message.answer("Выберите ваш пол:", reply_markup=gender_keyboard())

@dp.message(ProfileFSM.gender)
async def fsm_gender(message: Message, state: FSMContext):
    t = (message.text or "").strip().lower()
    if t.startswith("муж"):
        g = "M"
    elif t.startswith("жен"):
        g = "F"
    else:
        await message.answer("Пожалуйста, выберите кнопкой: Мужчина или Женщина.")
        return
    await state.update_data(gender=g)
    await state.set_state(ProfileFSM.looking_for)
    await message.answer("Кого вы хотите искать?", reply_markup=looking_for_keyboard())

@dp.message(ProfileFSM.looking_for)
async def fsm_looking_for(message: Message, state: FSMContext):
    t = (message.text or "").strip().lower()
    if "мужчин" in t:
        lf = "M"
    elif "женщин" in t:
        lf = "F"
    elif "кого угодно" in t:
        lf = "ANY"
    else:
        await message.answer("Выберите: Ищу мужчин / Ищу женщин / Ищу кого угодно.")
        return
    await state.update_data(looking_for=lf)
    await state.set_state(ProfileFSM.description)
    await message.answer(
        "Напишите описание анкеты (увлечения, интересы, чего ищете):",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Отмена")]], resize_keyboard=True),
    )

@dp.message(ProfileFSM.description)
async def fsm_description(message: Message, state: FSMContext):
    desc = (message.text or "").strip()
    if not desc:
        await message.answer("Описание не может быть пустым. Напишите описание:")
        return
    await state.update_data(description=desc)
    await state.set_state(ProfileFSM.photo)
    await message.answer(
        "Отправьте одно фото для анкеты.",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Отмена")]], resize_keyboard=True),
    )

@dp.message(ProfileFSM.photo, F.photo)
async def fsm_photo(message: Message, state: FSMContext):
    photo = message.photo[-1]
    file_id = photo.file_id
    data = await state.get_data()
    embed = await get_text_embedding(data["description"])
    await upsert_profile(
        message.from_user.id,
        username=message.from_user.username,
        name=data["name"],
        age=data["age"],
        city=data["city"],
        gender=data["gender"],
        looking_for=data["looking_for"],
        description=data["description"],
        photo_file_id=file_id,
        embedding=embed,
    )
    await state.clear()
    p = await get_profile(message.from_user.id)
    await message.answer_photo(
        photo=p["photo_file_id"],
        caption="Анкета сохранена:\n\n" + profile_caption(p),
        reply_markup=main_menu(),
    )

@dp.message(ProfileFSM.photo)
async def fsm_photo_invalid(message: Message):
    await message.answer("Пожалуйста, отправьте фото для анкеты.")

# -------- Просмотр своей анкеты --------

@dp.message(F.text == "Посмотреть мою анкету")
async def show_my_profile(message: Message):
    p = await get_profile(message.from_user.id)
    if not p or not is_profile_complete(p):
        await message.answer("Анкета не найдена или неполная. Нажмите «Создать/Редактировать анкету».")
        return
    await message.answer_photo(
        photo=p["photo_file_id"],
        caption=profile_caption(p),
    )

# -------- Редактирование по полям --------

@dp.message(F.text == "Редактировать анкету")
async def edit_menu(message: Message, state: FSMContext):
    p = await get_profile(message.from_user.id)
    if not p:
        await message.answer("Сначала создайте анкету.")
        return
    await message.answer("Что хотите изменить?", reply_markup=edit_fields_keyboard())

@dp.message(F.text == "Готово")
async def done_edit(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Готово.", reply_markup=main_menu())

@dp.message(F.text.in_(("Имя", "Возраст", "Город", "Пол", "Кого ищу", "Описание", "Фото")))
async def edit_field_entry(message: Message, state: FSMContext):
    p = await get_profile(message.from_user.id)
    if not p:
        await message.answer("Сначала создайте анкету.")
        return
    field_map = {
        "Имя": "name",
        "Возраст": "age",
        "Город": "city",
        "Пол": "gender",
        "Кого ищу": "looking_for",
        "Описание": "description",
        "Фото": "photo_file_id",
    }
    field = field_map[message.text]
    await state.set_state(EditFSM.value_input)
    await state.update_data(edit_field=field)
    if field == "gender":
        await message.answer("Выберите пол:", reply_markup=gender_keyboard())
    elif field == "looking_for":
        await message.answer("Кого хотите искать?", reply_markup=looking_for_keyboard())
    elif field == "photo_file_id":
        await message.answer(
            "Отправьте новое фото.",
            reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Отмена")]], resize_keyboard=True),
        )
    elif field == "age":
        await message.answer(
            "Введите возраст (18-99):",
            reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Отмена")]], resize_keyboard=True),
        )
    elif field in ("name", "city", "description"):
        await message.answer(
            "Введите новое значение:",
            reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Отмена")]], resize_keyboard=True),
        )

@dp.message(EditFSM.value_input, F.text.casefold() == "отмена")
async def cancel_edit_value(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Отменено.", reply_markup=main_menu())

@dp.message(EditFSM.value_input, F.photo)
async def set_new_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    if data.get("edit_field") != "photo_file_id":
        return
    file_id = message.photo[-1].file_id
    await upsert_profile(message.from_user.id, photo_file_id=file_id)
    await state.clear()
    await message.answer("Фото обновлено.", reply_markup=main_menu())

@dp.message(EditFSM.value_input)
async def set_new_value(message: Message, state: FSMContext):
    data = await state.get_data()
    field = data.get("edit_field")
    txt = (message.text or "").strip()

    if field == "gender":
        t = txt.lower()
        if t.startswith("муж"):
            await upsert_profile(message.from_user.id, gender="M")
        elif t.startswith("жен"):
            await upsert_profile(message.from_user.id, gender="F")
        else:
            await message.answer("Выберите кнопкой: Мужчина / Женщина.")
            return
    elif field == "looking_for":
        t = txt.lower()
        if "мужчин" in t:
            await upsert_profile(message.from_user.id, looking_for="M")
        elif "женщин" in t:
            await upsert_profile(message.from_user.id, looking_for="F")
        elif "кого угодно" in t:
            await upsert_profile(message.from_user.id, looking_for="ANY")
        else:
            await message.answer("Выберите из меню предпочтений.")
            return
    elif field == "age":
        try:
            age = clamp_age(int(txt))
        except Exception:
            await message.answer("Введите число 18-99.")
            return
        await upsert_profile(message.from_user.id, age=age)
    elif field == "name":
        if not txt:
            await message.answer("Имя не может быть пустым.")
            return
        await upsert_profile(message.from_user.id, name=txt)
    elif field == "city":
        if not txt:
            await message.answer("Город не может быть пустым.")
            return
        await upsert_profile(message.from_user.id, city=txt)
    elif field == "description":
        if not txt:
            await message.answer("Описание не может быть пустым.")
            return
        emb = await get_text_embedding(txt)
        await upsert_profile(message.from_user.id, description=txt, embedding=emb)
    else:
        await message.answer("Неизвестное поле.")
        return

    await state.clear()
    await message.answer("Обновлено.", reply_markup=main_menu())

# -------- Предпочтения поиска --------

@dp.message(F.text == "Предпочтения поиска")
async def prefs(message: Message, state: FSMContext):
    p = await get_profile(message.from_user.id)
    if not p:
        await message.answer("Сначала создайте анкету.")
        return
    lf = (p.get('looking_for') or 'ANY')
    txt = 'Мужчин' if lf == 'M' else ('Женщин' if lf == 'F' else 'Кого угодно')
    await message.answer(
        f"Текущие предпочтения: {txt}\nВыберите новое:",
        reply_markup=looking_for_keyboard(),
    )
    await state.set_state(EditFSM.field_choice)
    await state.update_data(edit_field="looking_for")

@dp.message(EditFSM.field_choice, F.text.in_(("Ищу мужчин", "Ищу женщин", "Ищу кого угодно")))
async def set_pref_looking_for(message: Message, state: FSMContext):
    t = message.text.lower()
    if "мужчин" in t:
        lf = "M"
    elif "женщин" in t:
        lf = "F"
    else:
        lf = "ANY"
    await upsert_profile(message.from_user.id, looking_for=lf)
    txt = 'Мужчин' if lf == 'M' else ('Женщин' if lf == 'F' else 'Кого угодно')
    await state.clear()
    await message.answer(f"Предпочтения обновлены: {txt}", reply_markup=main_menu())

# -------- Поиск/Лайки --------

@dp.message(F.text == "Поиск анкет")
async def start_search(message: Message, state: FSMContext):
    p = await get_profile(message.from_user.id)
    if not p or not is_profile_complete(p):
        await message.answer("Сначала создайте и заполните анкету (все поля и фото).")
        return
    await show_next_candidate(message.chat.id, message.from_user.id)

async def show_next_candidate(chat_id: int, user_id: int):
    candidates = await find_candidates(user_id, limit=CANDIDATES_LIMIT)
    if not candidates:
        await bot.send_message(chat_id, "Пока нет подходящих анкет. Попробуйте позже или измените предпочтения.")
        return
    c = candidates[0]
    await bot.send_photo(
        chat_id=chat_id,
        photo=c["photo_file_id"],
        caption=profile_caption(c),
        reply_markup=profile_inline_kb(),
    )

async def notify_user_about_likes(target_user_id: int):
    n = await count_pending_likers(target_user_id)
    if n <= 0:
        return
    try:
        await bot.send_message(
            chat_id=target_user_id,
            text=f"Вашу анкету лайкнул {n} человек",
            reply_markup=show_likers_kb(),
        )
    except Exception as e:
        logger.warning(f"Не удалось отправить уведомление о лайках пользователю {target_user_id}: {e}")

@dp.callback_query(F.data.in_(("like", "dislike")))
async def on_like_dislike(call: CallbackQuery):
    user_id = call.from_user.id
    p = await get_profile(user_id)
    if not p:
        await call.answer("Сначала создайте анкету.", show_alert=True)
        return

    # Берем актуального первого кандидата из общего поиска
    candidates = await find_candidates(user_id, limit=1)
    if not candidates:
        await call.answer("Подходящих анкет нет.", show_alert=True)
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        return
    cand = candidates[0]
    action = "like" if call.data == "like" else "dislike"
    await record_interaction(user_id, cand["user_id"], action)

    if action == "like":
        # Проверка взаимности
        mutual = await has_interaction(cand["user_id"], user_id, "like")
        if mutual:
            text_for_me = "Вы понравились:\n\n" + profile_caption(cand, include_username=True)
            await call.message.answer_photo(photo=cand["photo_file_id"], caption=text_for_me)

            me = await get_profile(user_id)
            try:
                text_for_them = "Вы понравились:\n\n" + profile_caption(me, include_username=True)
                await bot.send_photo(chat_id=cand["user_id"], photo=me["photo_file_id"], caption=text_for_them)
            except Exception as e:
                logger.warning(f"Не удалось уведомить вторую сторону о взаимном лайке: {e}")
        else:
            await notify_user_about_likes(cand["user_id"])

    await call.answer("Сохранено.")
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await show_next_candidate(call.message.chat.id, user_id)

# -------- Просмотр лайкнувших --------

@dp.callback_query(F.data == "show_likers")
async def cb_show_likers(call: CallbackQuery):
    await call.answer()
    await show_next_liker(call.message.chat.id, call.from_user.id)

async def show_next_liker(chat_id: int, user_id: int):
    liker = await get_next_pending_liker(user_id)
    if not liker:
        await bot.send_message(chat_id, "Лайкнувших больше нет.", reply_markup=None)
        await bot.send_message(chat_id, "Перейти к просмотру анкет:", reply_markup=go_to_search_kb())
        return
    await bot.send_photo(
        chat_id=chat_id,
        photo=liker["photo_file_id"],
        caption=profile_caption(liker),
        reply_markup=likers_inline_kb(liker["user_id"]),
    )

@dp.callback_query(F.data.startswith("liker_like:"))
async def cb_liker_like(call: CallbackQuery):
    user_id = call.from_user.id
    try:
        liker_id = int(call.data.split(":", 1)[1])
    except Exception:
        await call.answer("Ошибка.", show_alert=True)
        return

    pending = await has_interaction(liker_id, user_id, "like")
    if not pending:
        await call.answer("Этот пользователь больше не в списке.", show_alert=True)
        try:
            await call.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await show_next_liker(call.message.chat.id, user_id)
        return

    await record_interaction(user_id, liker_id, "like")

    liker_profile = await get_profile(liker_id)
    me = await get_profile(user_id)
    if liker_profile and me:
        text_for_me = "Вы понравились:\n\n" + profile_caption(liker_profile, include_username=True)
        await call.message.answer_photo(photo=liker_profile["photo_file_id"], caption=text_for_me)
        try:
            text_for_them = "Вы понравились:\n\n" + profile_caption(me, include_username=True)
            await bot.send_photo(chat_id=liker_id, photo=me["photo_file_id"], caption=text_for_them)
        except Exception as e:
            logger.warning(f"Не удалось уведомить лайкнувшего о взаимности: {e}")

    await call.answer("Лайк!")
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await show_next_liker(call.message.chat.id, user_id)

@dp.callback_query(F.data.startswith("liker_dislike:"))
async def cb_liker_dislike(call: CallbackQuery):
    user_id = call.from_user.id
    try:
        liker_id = int(call.data.split(":", 1)[1])
    except Exception:
        await call.answer("Ошибка.", show_alert=True)
        return

    await record_interaction(user_id, liker_id, "dislike")

    await call.answer("Дизлайк.")
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await show_next_liker(call.message.chat.id, user_id)

@dp.callback_query(F.data == "stop_likers")
async def cb_stop_likers(call: CallbackQuery):
    await call.answer("Остановлено.")
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await call.message.answer("Перейти к просмотру анкет:", reply_markup=go_to_search_kb())

@dp.callback_query(F.data == "go_to_search")
async def cb_go_to_search(call: CallbackQuery):
    await call.answer()
    await show_next_candidate(call.message.chat.id, call.from_user.id)

# -------- Прочие коллбэки --------

@dp.callback_query(F.data == "my_profile")
async def on_my_profile_cb(call: CallbackQuery):
    p = await get_profile(call.from_user.id)
    if not p or not is_profile_complete(p):
        await call.answer("Анкета не найдена.", show_alert=True)
        return
    await call.message.answer_photo(photo=p["photo_file_id"], caption=profile_caption(p))
    await call.answer()

@dp.callback_query(F.data == "stop_search")
async def on_stop_search(call: CallbackQuery):
    await call.answer("Поиск остановлен.")
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await call.message.answer("Что дальше?", reply_markup=main_menu())

# -------- Виртуальный собеседник --------

@dp.message(F.text == "Виртуальный собеседник")
async def virtual_entry(message: Message, state: FSMContext):
    await state.set_state(VirtualChatFSM.choose_partner)
    await message.answer(
        "Кого хотите в собеседники?",
        reply_markup=virtual_partner_keyboard(),
    )

@dp.message(VirtualChatFSM.choose_partner, F.text.in_(("Виртуальный мужчина", "Виртуальная женщина")))
async def virtual_choose(message: Message, state: FSMContext):
    partner_gender = "M" if "мужчина" in message.text.lower() else "F"
    await set_virtual_state(message.from_user.id, partner_gender, [])
    await state.set_state(VirtualChatFSM.chatting)
    await message.answer(
        "Готово! Напишите сообщение виртуальному собеседнику.\n"
        "Команда: «Закончить виртуальный чат» — чтобы завершить.",
        reply_markup=virtual_partner_keyboard(),
    )

@dp.message(F.text == "Закончить виртуальный чат")
async def virtual_end(message: Message, state: FSMContext):
    await set_virtual_state(message.from_user.id, None, [])
    await state.clear()
    await message.answer("Виртуальный чат завершён.", reply_markup=main_menu())

@dp.message(F.text == "Назад в меню")
async def back_to_menu(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Меню:", reply_markup=main_menu())

@dp.message(VirtualChatFSM.chatting)
async def virtual_chatting(message: Message, state: FSMContext):
    p = await get_profile(message.from_user.id)
    if not p:
        await message.answer("Сначала создайте анкету.")
        return
    partner_gender, history = await get_virtual_state(message.from_user.id)
    if not partner_gender:
        await message.answer("Сначала выберите виртуального собеседника.")
        await state.set_state(VirtualChatFSM.choose_partner)
        return
    history.append({"role": "user", "content": (message.text or "").strip()})
    answer = await virtual_reply(p, partner_gender, history, message.text or "")
    history.append({"role": "assistant", "content": answer})
    history = history[-20:]
    await set_virtual_state(message.from_user.id, partner_gender, history)
    await message.answer(answer, reply_markup=virtual_partner_keyboard())

# =========================
# Команды
# =========================

@dp.message(Command("my"))
async def cmd_my(message: Message):
    await show_my_profile(message)

@dp.message(Command("search"))
async def cmd_search(message: Message, state: FSMContext):
    await start_search(message, state)

# =========================
# Запуск
# =========================

async def main():
    from ai_utils import aclose_http_client
    await init_db()
    logger.info("Бот запускается...")
    try:
        await dp.start_polling(bot, allowed_updates=["message", "callback_query"])
    finally:
        try:
            await aclose_http_client()
        except Exception as e:
            logger.warning(f"Ошибка при закрытии HTTP-клиента OpenAI: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):

        logger.info("Бот остановлен.")
