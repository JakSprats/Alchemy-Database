function add_lcap(fk1)
  print ('add_lcap(' .. fk1 .. ')');
  return "ADDED";
end
function del_lcap(fk1, fk2)
  print ('del_lcap(' .. fk1 .. ',' .. fk2 .. ')');
  return "DELETED";
end
